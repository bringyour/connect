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

## 12. UDP: the buffer that is not a lock, the abandon path it never enters, and the shape of a UDP zombie

Design, 2026-09-13. H8, H9 and the converse the brief names as H10.
Kernel numbers from the runner (`7.0.12-linuxkit`); the source is
`UdpSequence.openSocket`, `ip_udp_socket_poller.go`, `retryReturnSend`,
`providerReturnIpTransferOptions`, `UdpBuffer.runSharedSocketLifecycle`
and `setSourceRetired`.

### 12.1 H8: what the UDP buffer request does

`openSocket` sets both buffers after the dial from the UDP
`MaxWindowSize`, `MemoryScaledByteCount(1 MiB, 256 KiB)`, so 1 MiB unscaled
and 256 to 768 KiB on the phone profiles (§11.1). UDP has no autotuning,
so there is nothing to lock: the call is exactly what it looks like, a
request for a bigger buffer than the default. The kernel clamps it to
`net.core.{r,w}mem_max` and doubles it. Observed:

| Request | `rmem_max` | `SO_RCVBUF` | 1,400-byte datagrams that fit with the reader paused | payload fraction |
|---:|---:|---:|---:|---:|
| none | any | 212,992 (the default, not doubled) | 92 | 0.60 |
| 256 KiB | 4 MiB | 524,288 | 227 | 0.61 |
| 1 MiB | 4 MiB | 2,097,152 | 910 | 0.61 |
| 1 MiB | 212,992 (stock) | 425,984 | about 185 (by the same charge) | 0.61 |

Each 1,400-byte datagram is charged 2,304 bytes against the buffer (its
`skb` truesize), so the payload capacity is 0.60 of the reported number;
100-byte datagrams are charged 896 and fit at 0.11, 8 KiB datagrams at
0.49. `SO_MEMINFO`'s drop counter equals sent minus delivered exactly
(5,081 of 5,991; 516 of 608). This is the trap the handover flagged, and
it is worse than "half": a drop-counter test must size by the charge, not
by the payload.

Three consequences. On a stock host the request is inert above 208 KiB,
so every profile gets 425,984 and the memory scaling changes nothing; the
call still doubles the default's capacity and stays. On a host with
`rmem_max` raised the request lands, and the unscaled 2 MiB holds 2.1 ms
of a 1 Gb/s arrival, the stock 425,984 about 0.4 ms. And the drop is
invisible: the poller reads what the kernel queued, and the flow above it
sees a gap it cannot distinguish from loss on the origin path.

Decision: the UDP calls stay, with their comment saying what they buy
(twice the default on a stock host, the request itself where the operator
allows it) and that nothing here can be locked. Removing them for symmetry
with §9 would halve the buffer on every stock host. What the program adds
is the instrument: every UDP flow reads its socket's own drop counter as
it closes and adds it to `LocalUserNat.UdpKernelReceiveDropCount()`,
Linux only through `SO_MEMINFO` (`ip_udp_socket_drops_linux.go`), one
getsockopt per flow close and nothing on the packet path. Landed in
`ip: count the datagrams the kernel drops at a UDP flow's socket`.

Prediction for the UDP cell, stated before it runs: with the provider's
read shards keeping up, kernel drops are zero at 200, 400 and 600 Mb/s of
UDP download on the rig; drops appear only as bursts when a shard is
descheduled for longer than the buffer holds, so a run that shows them
shows them clustered, not spread, and their count is (pause − 0.4 ms) ×
rate ÷ 2,304 on a stock host. If drops are spread evenly at a rate that
grows with offered load, the shards are not keeping up and the fix is
`SocketReadShardCount`, not the buffer.

An adjacent finding, not fixed here: each poller shard reads into a
`ReadBufferByteCount` buffer of 2,048 bytes (`defaultUdpReadBufferByteCount`),
and a UDP `read` into a short buffer discards the rest of the datagram. A
datagram over 2 KiB from an origin, which EDNS permits up to 4,096 and
which some tunnels and games send, is truncated silently. Rare on the
public internet because such datagrams fragment at the IP layer first,
but a provider on a jumbo path would see it. Row D5 pins the current
behaviour.

### 12.2 H9: UDP never reaches the abandon path, and what its zombie is instead

Positively: a UDP return is delivered to the provider with
`receiveRecoveryModeNonblocking` (`UdpSequence.receiveBatch`), and
`retryReturnSend` returns after the first attempt for any item whose mode
is not `receiveRecoveryModeTcpSocket`, before either abandon evaluation.
`returnSendAckEvidence` also excludes it, so a UDP item is neither
counted outstanding nor able to record a stall. The abandon timeout is
structurally TCP-only, and the reporter's fix is correctly bounded.

The converse, what a UDP flow does when its client is gone:

1. Its return datagrams are sent NoAck (`providerReturnIpTransferOptions`
   clears `Ack` for a non-TCP item under `ForceStream`), so they bypass
   the resend queue: written once to the transport, forwarded by the
   exchange into a forward whose destination has no resident, and dropped
   there when that forward's buffer fills (`ForwardTimeout` is 0 in
   production, so the drop is nonblocking). A UDP zombie retransmits
   nothing. Its egress is whatever the origin keeps sending, at the
   origin's rate, until the origin stops; most UDP protocols stop within
   seconds without feedback, a one-way stream does not.
2. Its socket, poller registration and bounded send queue live until
   `IdleTimeout` after the last socket activity, 300 s on the provider
   profile (`providerUdpIdleTimeout`), swept by `runSharedSocketLifecycle`
   or the per-flow idle timer. That is the leak, and it is bounded by
   origin activity plus 300 s, not by anything the client does.
3. If the same client also has a parked TCP return, the TCP abandon
   releases the whole source, and `UdpBuffer.setSourceRetired` cancels
   every UDP sequence of that source at once.

So there is no UDP counterpart to the TCP zombie's cost: nothing is
retransmitted and nothing is held past the idle reaper. The one thing
worth measuring is the exchange's `forwardDroppedCounter` under a
one-way UDP stream to a dead client, which should climb at the origin's
packet rate for up to 300 s and then stop.

### 12.3 Tests, in the contract shape

| Row | Test | Pins | Fails on | Regime |
|---|---|---|---|---|
| D1 | `TestUpstreamUdpBufferSizing` (Linux only) | after `openSocket`, `SO_RCVBUF` and `SO_SNDBUF` read `2 × min(MaxWindowSize, net.core.{r,w}mem_max)`, with the sysctls read from `/proc`, under `SetMemoryBudget(0)` and `SetMemoryBudget(8 MiB)` | a tree that removes the UDP calls "for symmetry", where both read the 212,992 default | Linux |
| D2 | `TestUpstreamUdpReceiveDropsAreCounted` (Linux only) | a UDP flow whose reader is held while a loopback origin sends `4 × SO_RCVBUF / 1,400` datagrams of 1,400 bytes: after the flow closes, `LocalUserNat.UdpKernelReceiveDropCount()` equals sent minus delivered, and delivered is within 10 per cent of `SO_RCVBUF / 2,304`, the charge of 12.1 | main, which has no counter; and any sizing by payload rather than charge | Linux |
| D3 | `TestUdpReturnNeverEntersTheAbandonPath` | a UDP return toward a destination whose sequence admits nothing: exactly one attempt (`afterReturnSendAttemptForTest` once, sent = false), the producer returns at once, no release for 5 T, and the source's evidence records nothing outstanding and no stall | a tree that lets a datagram item into the retry loop | in-process |
| D4 | `TestUdpFlowReleasedWhenClientDisappears` | a UDP flow with the client gone: (a) the flow closes `IdleTimeout` after the origin's last datagram, at test scale, and its socket is unregistered; (b) with a parked TCP return of the same source released by the abandon, `setSourceRetired` cancels the UDP sequence before the release completes | none by design; characterises the bound | in-process |
| D5 | `TestUdpDatagramOverTheReadBufferIsTruncated` | characterisation of the 2 KiB read: a 4,000-byte origin datagram reaches the flow as 2,048 bytes | none; documents | in-process, loopback |

The UDP cell that calls H8 answered: UDP download through the provider at
200, 400 and 600 Mb/s, four repetitions, main against this tree (identical
sockets, so identical throughput is the prediction), with
`UdpKernelReceiveDropCount` read at the end of each run and the exchange's
forward drop counter beside it.

## 13. The coupling: what a dead client's sequence puts on the wire, and what to measure before naming a resource

Design, 2026-09-13. H5 and H6. Nothing here names the resource; it names
the measurement that will, the prediction it is held to, and the
instrument landed for it.

### 13.1 The arithmetic, and the prediction

The report: zombies retransmit about 8 Mb/s each and forty of them take a
639 Mb/s provider to 180. Forty times eight is 320 and the loss is about
460, so bandwidth does not obviously account for it. Before measuring
anything, what the source says a zombie should cost.

A client that dies mid-download leaves its return sequence with a full
resend queue of retained items (`retainAfterAckTimeout`), and with the
lane rule off, which is how main ships, every item is rewritten when its
timer fires. `resendIntervalForPolicy` doubles the interval per rewrite
from the scaled round trip to `MaxResendInterval`, 8 s, which every item
reaches by its sixth rewrite, within about twenty seconds of the death.
The queue is bounded by `ResendQueueMaxByteCount`, 2 MiB unscaled on one
lane (`LogicalDataLaneCount` 0), so the steady state is the whole queue
rewritten once per 8 s:

**Prediction: 2 MiB / 8 s ≈ 2.1 Mb/s per dead client, plus Transfer
framing, on an unscaled provider, from about twenty seconds after the
death.** Per client, not per flow: every flow of one client shares the
sequence to it. Forty dead clients are about 84 Mb/s, 13 per cent of the
provider's 640, which cannot be the 460.

Two readings of the report's "8 Mb/s each" and what each would mean. If
it is per client at steady state, the interval is not reaching its ceiling
and is sitting near 2 s, the cold floor, and bandwidth then does account
for most of the loss (320 of 460, before framing); that is a timer finding
in the transfer layer, and row Z1 below would fail on main. If it is per
flow of one client, it cannot be resend egress at all, since one sequence
carries them, and something else was measured. The split below decides
which, and it is the first thing to run.

### 13.2 The instrument, landed

`Client.DestinationSendStats(destinationId)` sums, over the live send
sequences to one destination, `WriteCount`, `WriteByteCount`,
`ResendWriteCount` and `ResendWriteByteCount`: first writes and recovery
rewrites counted at the write, as per-sequence atomics, so a destination's
egress splits into delivery and retransmission with no allocation and no
lock on the path (`transfer: count what each send sequence writes`). The
measurement stream reads it per killed client id and per live client id
at intervals; the difference of two readings is the rate.

Already available beside it: `Client.ResendQueueSize` per destination;
the shared budget's `UsedByteCount()` against `TotalByteCount()` when a
`ResendQueueBudget` is set; `MessagePoolStats()`; the client's
`TimeoutResendWriteCount`, `RouteUnacknowledgedDuration` and
`RouteRetainedItemCount`; on the exchange, `forwardDroppedCounter` and
`abuseDroppedCounter`; and the provider host's CPU.

### 13.3 The candidates, with what each predicts

The threshold shape is 8 zombies at 719, 16 at 587, 40 at 180 from 639,
which is 11, 27 and 72 per cent: about 1.4 to 1.8 per cent, 10 to 12 Mb/s
of live throughput, per zombie, roughly linear and steepening. That is
four to five times a zombie's own predicted egress, so whatever it is
costs more than its bytes.

- The shared resend budget, on an sdk-hosted provider only.
  `configureDeviceLocalProviderMemory` gives every sequence one budget of
  three sevenths of half the provider target, 4.3 MiB at the 20 MiB
  desktop default, with `ResendQueueMaxByteCount` as the borrow cap and
  256 KiB as the guaranteed floor. A zombie holds its borrow forever, so
  two or three exhaust the budget and every live sequence is held at its
  256 KiB floor: a per-flow ceiling of 256 KiB × 8 / RTT, 100 Mb/s at
  20 ms. That predicts a cliff at two or three zombies, not a slope from
  eight, which is one reason to think the reporter's provider is a bare
  one with independent per-sequence budgets, where this candidate does
  not exist. `UsedByteCount` against `TotalByteCount` decides it in one
  reading.
- The transport write path. Every rewrite is framed and encrypted again
  and written into the same carrier as live traffic. At 84 Mb/s that is a
  13 per cent share of bytes and of the encryption CPU, a slope of about
  a third of a per cent per zombie. Too shallow by five times unless the
  interval is stuck at 2 s.
- The exchange. Forwards are nonblocking in production (`ForwardTimeout`
  0), so a dead destination's full forward buffer drops and never blocks
  the resident's ingress shard; there is no head-of-line coupling there.
  The exchange does spend CPU framing what it drops, and
  `forwardDroppedCounter` climbing at the zombies' packet rate is the
  signature.
- Message pools. Forty resend queues pin about 80 MiB of pool buffers;
  if the pool's memory target is below that, live flows' gets fall
  through to the heap and the GC pays. `MessagePoolStats()` shows the
  occupancy; this predicts a slope that steepens, which is the shape.
- Not candidates, from the source: the provider's return admission
  (parked producers wait on their own sequence's notify and zombies of
  different clients share none of it); the contract manager (a full
  window requests nothing); the exchange's forward limit per resident,
  8,192, far above forty.

### 13.4 The matrix

Zombie count in {0, 8, 16, 40}, the same live client set throughout, four
repetitions, on two provider hosts: a bare provider and an sdk-hosted one
at the 20 MiB desktop target. Recorded per run: live throughput; per
destination, the `DestinationSendStats` split for every killed id and
every live id, sampled every 5 s; the resend budget's used and total where
one exists; `MessagePoolStats`; the exchange's forward drops; provider
and exchange CPU; and the kernel's `ss -tmi` on the provider's transport
socket. The resource named is the one whose counter crosses a bound at
the same zombie count where live throughput falls; a candidate whose
counter moves linearly while throughput falls super-linearly is not it.

What the split must show for 13.1 to stand: killed ids at 2.1 Mb/s of
rewrites each (± 0.5) from twenty seconds after the kill, live ids with
rewrites under 2 per cent of their first writes. If killed ids show
8 Mb/s, 13.1 is wrong, the timer is the finding, and row Z1 says where.

### 13.5 Tests, in the contract shape

| Row | Test | Pins | Fails on | Regime |
|---|---|---|---|---|
| Z1 | `TestZombieFlowEgressIsBounded` | one send sequence to a sink that acknowledges nothing, lane rule off, `ResendQueueMaxByteCount` Q = 256 KiB, `MaxResendInterval` M = 200 ms; once every item has been written seven times, `ResendWriteByteCount` over a window W = 5 M lies within [0.5, 1.25] × Q × W / M | neither tree, by 13.1; a ratchet in §36.13's sense, and the row that fails on main if "8 Mb/s each" is the steady state | transfer layer, in-process |
| Z2 | `TestDestinationSendStatsSplitFirstWritesFromRewrites` | N packs to one destination written once and then each rewritten once: `WriteByteCount` equals the first-write bytes, `ResendWriteByteCount` the rewrite bytes, `SequenceCount` 1; a second destination reads zero | a tree without the counters | in-process |
| Z3 | `TestZombieRewritesStopAtRelease` | the same sequence under a provider whose source is released by §10: `ResendWriteByteCount` stops advancing within one M of the release | main only if its release does not join the sequence, which it does; a guard | in-process |

## 14. What this round corrects in sections 1 to 8

Recorded so the earlier text is read with these in hand rather than
edited under them.

1. §2's table and §4 treat the platform's Reliability verdict as the
   platform's answer to "is the destination active". It is the answer to
   "is the destination's identity retired": `contractDestinationActive`
   reads `network_client.active`, which an offline client keeps. A
   contract probe cannot detect a dropped client (§10.2).
2. §4 offers the 209, 264 and 723 s wedges as evidence against a 120 s
   bound. They were produced with `ReliableLaneProvenRecovery` on, which
   shipped off after that measurement with 0 of 120 wedges off against 8
   of 120 on. Nothing measured on the shipped tree is silent for 120 s;
   the longest is 36 s (§10.3). The bound's danger was real and lay
   elsewhere: in the quantity it was taken over (§10.1).
3. §3's "possibly 256 KiB" does not occur on a shipping host; the phone
   profiles request 2 to 8 MiB (§11.1). On a stock Linux host the request
   is clamped to 208 KiB whatever the budget, so the memory scaling never
   reached the kernel and a phone provider was exactly as frozen as a
   server one (§11.2).
4. H1's second clause, that the receive deletion lowers throughput where
   `tcp_rmem[2]` is below what the explicit call requested, does not hold
   on Linux: the explicit call never raised the receive window at any
   size, because the window clamp was frozen at its SYN-time value and
   only autotuning moves it (§9.1's kernel path, §11.2). The buffer the
   old code obtained was larger than autotuning's ceiling only on a host
   with `rmem_max` raised, and there it still advertised the frozen
   window. The comparison H1 asks for is still worth one run for the
   record; it is not a landing condition.
5. The reporter's Linux-only test rationale, that macOS refuses an
   oversized set with `ENOBUFS` and keeps autotuning, is not what this
   macOS does: it clamps silently to `kern.ipc.maxsockbuf` and locks, and
   the mobile-profile requests are not oversized there at all (§11.2).
   The Darwin rows M2 and M3 follow.
6. §2's "the same pattern appears on the UDP socket" is right that it is
   the same call and wrong to leave it as a caution only: the UDP call is
   a real buffer increase on a stock host and stays (§12.1).
7. §5's row 5, `TestZombieFlowEgressIsBounded`, is kept but reclassified:
   the bound it asserts is what the timer already does, and the row's
   value is as the instrument that decides whether the report's 8 Mb/s
   is a steady state (§13.1).

## 15. The buffer rule: an explicit request only where it beats the kernel's own ceiling

Design and build, 2026-09-13, after the measurement round: twelve
campaigns, 490 runs. This supersedes §9.3's unconditional deletion and
applies to the receive side on main as well.

### 15.1 What the measurement showed, and what it means

The deletion of the send request against main, upload cell, by provider
budget: +363 to +403 per cent at the 1 MiB budget, 17 of 17 paired
repetitions; null at 8 MiB; −20.8 per cent at 32 MiB, 0 of 5 better,
p = 0.006; −11.5 per cent at the default inside a ±43 per cent null band.
The host has `net.core.wmem_max = 4 MiB` and `tcp_wmem[2] = 4 MiB`.

Read against §11.1's table this is one inequality. What an explicit
request obtains is `2 × min(request, wmem_max)` on Linux; what autotuning
reaches is `tcp_wmem[2]`. At 1 MiB the request is 256 KiB, obtains 512 KiB,
below the 4 MiB ceiling: the deletion wins by the ratio. At 8 MiB the
request is 2 MiB and obtains exactly the ceiling: null. At 32 MiB and at
the default the request obtains 8 MiB, above the ceiling: the pin was
carrying twice what autotuning may, and at the cell's round trip that was
a fifth of the upload. §9.3 said the bad case needs `wmem_max` raised
above `tcp_wmem[2]`; this host has them equal, and equal is enough,
because the doubling puts the obtained value above the ceiling. So §9.3's
"closer to unconditionally better" was wrong by exactly that doubling, and
the measurement caught it before it landed.

The receive side has the same exposure, and one more finding sharpens it.
On the runner's kernel (`7.0.12`) a post-connect `SO_RCVBUF` does **not**
freeze the window at its SYN-time value: with an 8 MiB pin `rcv_ssthresh`
grew to 8,354,736 under load, with the stock-sized pin to 415,524, against
919,873 to 1,052,478 unpinned, sized by autotuning to what a loopback flow
needed. The reporter's 3.2x was therefore a 415 KB pinned window against
autotuning on a stock host, and the same inequality decides its sign:
`2 × min(request, rmem_max)` against `tcp_rmem[2]`. On stock hosts
(425,984 against 6 MiB) the reporter's deletion wins, which is what was
measured; on a host with `rmem_max` at or above half of `tcp_rmem[2]` and
a path whose bandwidth-delay product exceeds `tcp_rmem[2]`, it loses. That
is a finding about the baseline on main, stated plainly: the receive fix
was measured on one host where the inequality happens to favour it, and
its sign flips on hosts where it does not, exactly as the send deletion's
did here. Neither direction is unconditionally better; both are decided
by numbers the process can read.

The kernel generation matters for one thing only. On the reporter's
kernel a post-connect receive pin froze the clamp near 64 KB; on `7.0.12`
it did not. A pre-connect pin sets the SYN-time clamp from the buffer on
every generation, so where a receive pin is right it is applied before
connect and never after.

### 15.2 The rule, as built

`socketBufferPolicy` (`upstream_socket_buffer.go`), read once from the
kernel: on Linux `net.core.{w,r}mem_max` and `tcp_{w,r}mem[2]` from
`/proc`, with `doubled`; on Darwin `kern.ipc.maxsockbuf` and
`net.inet.tcp.auto{snd,rcv}bufmax`; elsewhere unknown. Per direction,
`explicitSend(request)` and `explicitReceive(request)` are true exactly
when the obtained value, `min(request, coreMax)` doubled on Linux, exceeds
the ceiling; an unknown policy never pins. The request is
`TcpBufferSettings.MaxWindowSize`, as before.

Two application points. `DefaultTcpBufferSettingsWithBufferSize` sets
`ConnectSettings.DialControl`, a new hook the default dialer runs on every
socket before connecting (chained ahead of the egress binding control), to
`upstreamSocketBufferControl(request, policy)`: it sets `SO_SNDBUF` and
`SO_RCVBUF` to the request where the rule says so, and is nil when it says
nothing, so a host that pins nothing keeps no hook. `configureUpstreamTcpConn`
takes the request, the policy and whether the pre-connect hook ran, which
is `DialContextSettings == nil`; a host-supplied dial is opaque to the
hook, and there only the send pin is applied after connect, since a
post-connect receive pin is the generation-dependent freeze and is never
applied. Keepalive and no-delay stay.

On the measurement host this gives: 1 MiB budget, kernel in both
directions (the +380 per cent stands); 8 MiB, kernel (null stands); 32 MiB
and default, send pinned at 8 MiB pre-connect (the −20.8 per cent is
recovered, the −11.5 becomes the pin it measured), receive left to the
kernel since 8 MiB is under a 32 MiB `tcp_rmem[2]`. On a stock host,
kernel in both directions at every budget. On this macOS, 2 MiB requests
to the kernel and 8 MiB ones pinned, per §11.2's numbers.

Rejected: a fixed choice either way. Unconditional deletion costs a fifth
of upload on a host class that exists in the fleet and every tuned rig;
unconditional pinning costs 3.2x of download and 4x of upload on every
stock host. The condition is two integers the process reads once, and no
fixed choice is right on both. Also rejected: deciding from a bandwidth-
delay estimate, which would decide on an estimate where a fact is
available.

### 15.3 Tests, in the contract shape

The landed rows U1 and U2 (§9.4) and the receive rows assert "left to the
kernel", which is now the rule's answer only where it holds; they must
take an injected policy so their outcome does not depend on the host's
sysctls. The call sites moved to `configureUpstreamTcpConn(conn, request,
socketBufferPolicy{}, false)`, an unknown policy that never pins, which
keeps each row's meaning (nothing is set when the rule says kernel) and
its failure on the pre-fix code.

| Row | Test | Pins | Fails on | Regime |
|---|---|---|---|---|
| R1 | `TestSocketBufferRuleFollowsTheKernelCeiling` | the pure rule on the four measured points: request 256 KiB, 2 MiB, 8 MiB, 16 MiB against core max 4 MiB and ceiling 4 MiB doubled read kernel, kernel, explicit, explicit; against a stock 212,992 core max and 4 MiB ceiling all four read kernel; on an undoubled Darwin policy with max 8 MiB and ceiling 4 MiB, 2 MiB reads kernel and 8 MiB explicit; an unknown policy never pins | a tree without the rule | pure |
| R2 | `TestUpstreamDialPinsBothBuffersBeforeConnectWhenTheRuleSays` (Linux only) | a `TcpBufferSettings` whose `DialControl` is built from an injected policy that pins both at 8 MiB: after the dial `SO_SNDBUF` and `SO_RCVBUF` read `2 × min(8 MiB, rmem_max)` from `/proc`, and under 512 MiB of inbound load `rcv_ssthresh` (`TCP_INFO`) exceeds the stock clamp, which shows the pin was applied at SYN time | a tree that pins after connect on a kernel that freezes, or that never pins | Linux runner |
| R3 | `TestUpstreamDialLeavesBothBuffersWhenTheRuleSaysKernel` (Linux only) | the same dial under an injected policy that pins nothing: no `DialControl`, both buffers read their defaults after connect, and both grow under load to their `tcp_{w,r}mem[2]` | a tree that pins unconditionally | Linux runner |
| R4 | `TestOpaqueDialPinsOnlyTheSendBufferAfterConnect` | a `TcpBufferSettings` with a `DialContextSettings` dial and a policy that pins both: after `configureUpstreamTcpConn` the send buffer reads the pin and the receive buffer its default | a tree that pins receive after connect | any |
| R5 | `TestSocketBufferPolicyReadsThisKernel` (Linux and Darwin) | the process policy agrees with `/proc` or `sysctl` on the running host, and reads unknown where a value is missing | a mis-parsed sysctl | host |

The measurement that calls this landed: the same twelve campaigns on the
same host, where the rule must reproduce the 1 MiB gain, the 8 MiB null
and turn the 32 MiB loss into a null, and one campaign on a stock-sysctl
host where it must reproduce the deletion's gain at every budget. Both
before any of it merges.

## 16. The bistable receive window at 104,448 bytes: diagnosis plan

Debugging round, 2026-09-13. Not fixed here; this says what the state is,
what would produce it, and which readings decide between the candidates.

### 16.1 What the number is

104,448 is `131,072 × 204 / 256`: the default `tcp_rmem[1]` buffer through
the kernel's default scaling ratio, which is `tcp_win_from_space` of the
establishment-time receive buffer. It is the window the socket advertises
before autotuning has ever grown the buffer. 15.4 to 16.1 Mb/s is
104,448 × 8 divided by 52 to 54 ms, so the cell's round trip is about
50 ms and the flow is window-limited at its initial window for the whole
run. On both arms, since neither pins receive at that budget.

So the state is: `sk_rcvbuf` never grew past 131,072, or grew while
`rcv_ssthresh` was held at the initial window. Those are different
failures with different signatures.

### 16.2 The candidates and their signatures

`ss -tmi` on the upstream socket in a stuck run and in an engaged run of
the same cell, read twice ten seconds apart, decides among three:

1. Autotuning blocked outright: `skmem rb` stays 131,072 and `rcv_space`
   stays near its initial value (10 segments, about 14,600). Autotuning
   (`tcp_rcv_space_adjust`) grows the buffer only at a read at least one
   receiver round trip after the previous measurement and only when the
   bytes copied since exceed the previous measurement. It cannot run at
   all while the receiver's own round-trip estimate is zero, and it
   cannot grow while the socket is under memory pressure. Readings:
   `rcv_rtt` zero, or `rb` fixed with `rcv_space` fixed.
2. Autotuning engaged but the window held: `rb` and `rcv_space` grew,
   `rcv_ssthresh` stayed at 104,448. The advertised window grows in
   `tcp_grow_window` only while the socket is not under memory pressure
   and while the arriving segments are "efficient" (payload at least the
   window their truesize would buy). Readings: `rb` above 131,072 with
   `rcv_ssthresh` at 104,448; `nstat TcpExtTCPRcvCollapsed`,
   `TcpExtPruneCalled` and the `tcp_mem` pressure state on the host.
3. A lock set by something other than the provider: `rb` fixed at a
   value that is not the default. Readings: `SO_RCVBUF` on the socket
   against `tcp_rmem[1]`; the sdk's supplied dialer if any.

Candidate 1 is where the provider's own behaviour enters. The socket
reader parks in its first callback while the return sequence acquires a
contract, about 350 ms in earlier device measurements, and the origin
fills the initial window and stalls; when the reader resumes it drains
that window in a burst and then reads continuously at the delivery rate.
The receiver's round-trip estimate without timestamps is the time for one
advertised window to arrive, which a parked reader inflates by the park,
and the measurement interval follows that estimate. Whether the first
measurement after the park sees a burst larger than the previous
measurement, or a trickle smaller than it, depends on when the contract
arrived relative to the first data. That is a coin flip on timing, which
is the bistability's shape, and it is testable: log the first ten read
sizes and their timestamps on the upstream socket, and `TCP_INFO`'s
`rcv_rtt` and `rcv_space` after each, in a stuck and an engaged run. The
prediction to hold me to: stuck runs show `rcv_space` never exceeding
the first measurement's copied bytes and `rb` at 131,072; engaged runs
show `rcv_space` doubling within the first three measurements.

### 16.3 What follows from each

If candidate 1 holds, the fix is app-side and in the reader's start-up:
the first reads after the park must be one burst of at least the initial
window, which the reader can arrange by reading with a buffer at least the
initial window (the 32 MiB budget reads 32 KiB, the default 64 KiB, and
the initial window is 104 KiB; that budget dependence is itself a signal)
and by not returning to the socket until the queued batch is admitted, so
the next read is again a burst. `SO_RCVLOWAT` is not the tool: it holds
interactive responses until the low-water mark, and a fixed mark gives
autotuning one doubling and then equality, which does not grow. If
candidate 2 holds, it is host memory pressure and the provider is a
bystander. If candidate 3, it is the dial path.

Whatever holds, §15's rule already removes the exposure on hosts where a
receive pin beats the ceiling, because a pre-connect pin sets the clamp
without autotuning; it does nothing on stock hosts, where autotuning is
the only way to a large window and must be made to engage.

Row for the test stream once the signature is known:
`TestUpstreamReceiveWindowEngagesAfterAParkedFirstRead` (Linux only): a
loopback origin with 50 ms of netem delay is not available in process, so
this row asserts the reader's read pattern rather than the kernel's
response: after a first callback held for 300 ms, the reader's first read
returns at least the socket's queued bytes in one call, and the second
read does not occur before the first batch is admitted.

## 17. What exercises the abandon change

The upstream cell never abandons a flow, so 41d5045 has no measurement of
its own. Three shapes exercise it; the first is the reporter's and the
other two are the false positives it removes.

1. Dead clients. N clients downloading through the provider, killed with
   `SIGKILL` at t0 (no FIN, no RST from the client host: block the
   client's egress with a firewall rule before the kill so the transport
   dies silently). Record, per killed client id, `DestinationSendStats`
   every 5 s and the release time from the provider's log
   (`releaseUnreachableSource`). Prediction: every release lands between
   120 and 150 s after the client's last acknowledgement on this tree,
   120 to 210 s on main; the live clients' throughput recovers at the
   release on both.
2. A slow live client with many flows. One client shaped to 50 kb/s
   (`tc tbf` on its ingress) opening 40 concurrent downloads of 1 MiB
   each. Prediction: main releases and resets that client at least once
   inside five minutes (its inner connections fail with a reset and the
   downloads restart); this tree completes all 40 with no release. The
   other clients' throughput is unchanged on both.
3. A provider carrier outage. Twenty live clients downloading; the
   provider's exchange connection blackholed for 150 s (a firewall rule
   on the provider host toward the exchange, then removed). Prediction:
   main releases every source at about 120 s and every client's downloads
   reset when the carrier returns; this tree releases none, and the
   downloads resume where they stalled.

Each is a cell the measurement stream can build from its existing pieces;
the log line and the counters are the instruments, and 1 to 3 are also
the order in which a wrong prediction would be cheapest to learn from.

## 18. The TCP-path ceiling: one UDP flow at 1.34 Gb/s, one TCP flow at 0.3, sixteen at 0.7

The same provider, NAT and kernel socket deliver a single UDP flow
losslessly at 1.34 to 1.39 Gb/s, flat in flow count, and TCP at 0.26 to
0.33 Gb/s for one flow and 0.59 to 0.80 for sixteen. That excludes the
socket layer, dispatch, NAT flow tables and the device stack, and puts the
ceiling in what the TCP path does that the UDP path does not. From the
source, the differences are these, in the order I would measure them.

1. Synchronous admission on the flow's own reader. A TCP return is
   `receiveRecoveryModeTcpSocket`: the socket reader's batch is admitted
   to Transfer synchronously, and the reader does not read again until
   it is (`readPackets` holds at most `min(SequenceBufferSize,
   WriteBatchSize)`, 64 packets, of read-ahead). A UDP return is
   nonblocking and the poller shard never waits. So one TCP flow's rate is
   at most its read-ahead per admission latency: 64 packets of 1,500
   bytes is 96 KB, and at 0.3 Gb/s that is one admission every 2.5 ms.
   The instrument: the time each `retryReturnSend` attempt spends in
   `sendGroupWithTimeoutDetailed` on a socket-owned item, exported as a
   per-provider histogram, and the occupancy of `readPackets` when the
   reader blocks on it. Prediction: the admission wait is the flow's
   duty cycle; a single flow spends more than half its time in it.
2. The per-flow reliable window. A TCP flow's return items are `Ack`
   packs bounded by the destination's `ResendQueueMaxByteCount`, 2 MiB,
   and the client's tunnel-side acknowledgements pace it; UDP is NoAck
   and bypasses the queue. At the cell's round trip the 2 MiB bound is
   itself about 2 MiB × 8 / RTT, which at 50 ms is 335 Mb/s: the single-
   flow number. Sixteen flows to one client share one sequence and one
   bound, so they cannot exceed it together; sixteen flows to sixteen
   clients have sixteen bounds. The instrument is already there:
   `ReliableAdmissionWaitCount` and duration, and `ResendQueueSize` per
   destination. Prediction: with one client, sixteen flows read the same
   aggregate as one plus what the acknowledgement clock allows; with
   sixteen clients they scale. Whether the cell's sixteen flows share a
   client decides which reading it took, and the design must say which
   before the number is read.
3. Batch shape on the reliable carrier. A TCP return group is at most
   `providerReturnBatchMaxBytes`, 24 KiB, per admission; H1 then frames
   and encrypts per group. UDP datagrams ride the poller's shard batches.
   The instrument: frames per admission and bytes per H1 write, both
   countable at `sendReturnBatchWithLimits`.
4. The inner TCP itself: the NAT's window ladder toward the client
   (`InitialWindowSize` 1 MiB doubling to `MaxWindowSize`), its
   acknowledgement compression (`AckCompressTimeout` 50 ms), and the
   client's own receive window through a tun with the tunnel's round
   trip. UDP has none of these. The instrument: the NAT's advertised
   window and the client's, read from the packets, and the sequence's
   round-trip window (`RttWindow`).

The first measurement is the split between 1 and 2, because it needs no
new code: one client with sixteen flows against sixteen clients with one
flow each, with `ReliableAdmissionWaitDuration` and the per-destination
resend queue size beside the throughput. If sixteen clients scale and one
client does not, the ceiling is the per-destination reliable bound and
the follow-up is a per-destination lane count (`LogicalDataLaneCount`,
built and off, gives independent 2 MiB bounds per lane). If neither
scales, it is the reader's synchronous admission, and the follow-up is a
bounded admission queue between the socket reader and the sender on the
socket-owned path, which CODESTYLE allows for exactly this lane as "the
narrow shared-pump exception" provided the queue has independent byte and
count bounds, cancellation joins the worker, and every pooled buffer is
returned before lifecycle completion.

### 16.4 Measured in process: three reader shapes, none traps autotuning; the diagnosis redirects

Run on the runner's kernel with `lo` at MTU 1,500 and 25 ms of netem each
way (a 50 ms round trip, the cell's), `tcp_rmem` 4096 131072 33554432, a
loopback origin writing 24 MiB, reading `rcv_ssthresh`, `rcv_space`,
`rcv_rtt` and `SO_RCVBUF` from the socket:

| Reader | `rcvbuf` at the end | `rcv_space` | `rcv_rtt`, minimum seen | rate |
|---|---:|---:|---:|---:|
| reads 32 KiB continuously from the first byte | 30,678,545 | 1,456,688 | 51,000, 50,000 | 82.8 Mb/s |
| parks 300 ms, then reads 32 KiB continuously | 7,102,985 | 1,064,224 | 51,000, 50,000 | 57.8 Mb/s |
| parks 300 ms, then reads 64 KiB continuously | 25,080,056 | 4,063,232 | 50,000, 50,000 | 235.8 Mb/s |
| held to 12 Mb/s for 3 s, then free (twice) | 33,554,432 and 13,854,500 | 2.3 and 2.6 MB | 50,000 both | 413 and 265 Mb/s after the hold |
| held to 4 Mb/s for 4 s, then free | 131,072 through the hold, `rcv_space` 14,480; 5,245,453 after | 474,944 | 50,000 | 47.8 Mb/s after the hold |

Every shape engaged once the reader read freely. A parked first read does
not trap it (16.2's candidate 1 in its simple form is falsified), and the
receiver's round-trip estimate does not collapse under window-limited
bursts, which was the refinement that could have made the trap self-
sustaining; the minimum observed was exactly 50 ms in every run. The only
way the buffer stayed at 131,072 was a reader consuming less than the
initial `rcv_space` per round trip, which is autotuning sizing the window
to the application, as designed, and it engaged the moment consumption
rose.

So on this kernel a reader that drains a window per upstream round trip
engages autotuning, and a stuck window means the reader was not draining
a window per round trip. That contradicts the cell's arithmetic only if
the upstream round trip is the 50 ms that 104,448 × 8 / 16 Mb/s implies;
if the provider-to-origin round trip in the cell is the datacenter's few
milliseconds, the initial window alone would carry 150 to 400 Mb/s, and
16 Mb/s is not the upstream window's limit at all. Either way the reading
that decides is the same and it is cheap:

1. `ss -ti` on the provider's upstream socket in a stuck run: `rtt` (the
   upstream round trip) and `rcv_space` (bytes the reader copied per
   round trip). If `rtt` is milliseconds, the upstream window is a
   symptom and the bottleneck is downstream of the socket reader, in the
   tunnel. If `rtt` is 50 ms and `rcv_space` reads a full window per
   round trip with `rb` still 131,072, the kernel is doing something these
   runs did not show, and `nstat` for memory pressure and prune counters
   is next (16.2, candidate 2).
2. On the same run, the tunnel side of that flow: the NAT's advertised
   window toward the client (`TcpSequence.windowSize`, which starts at the
   memory-scaled `InitialWindowSize`, 512 KiB at 32 MiB, and doubles up
   the ladder), the client's own window on its tun interface, and the
   return sequence's `ReliableAdmissionWaitDuration` and resend queue
   size for that client. The prediction now: the stuck runs show the
   tunnel side holding the flow at 16 Mb/s from the first second, the
   upstream reader consuming exactly that, and the upstream window
   right-sized to it; the engaged runs differ on the tunnel side, not on
   the socket.

The budget dependence supports the redirection: the socket reader is the
same at every budget, while the tunnel-side windows, queues and pools are
what the 32 MiB budget scales. Row for the test stream, once the tunnel
reading is in: `TestReturnPathDoesNotHoldAFreshFlowAtItsInitialWindow`,
in process, asserting whichever tunnel-side bound the reading names grows
within the first round trips of a fresh flow.

### 15.4 The contradiction resolved: the cell and the runner agree, and the sentence was mine

§15.1 said that on the runner's kernel a post-connect `SO_RCVBUF` "does
not freeze the window at its SYN-time value". The measurement stream's
cell, with the receive line restored, reads an advertised window of
451,584 in six of six runs against 6,171,648 to 31,707,136 unmodified.
Those were read as opposites. They are the same reading.

Same kernel (`7.0.12-linuxkit`), same call (`SetReadBuffer(262144)` after
connect, the 1 MiB budget's request, the cell's), same instrument
(`tcp_info`, which is what `ss -ti` prints), loopback at MTU 1,500 with a
50 ms round trip:

| Arm | `rcvbuf` | `rcv_ssthresh` at the end, three runs |
|---|---:|---|
| pinned, 262,144 | 524,288 | 397,574, 394,460, 394,338 |
| unpinned | 16,786,971, 33,554,432, 30,925,017 | 12,782,745, 25,558,071, 23,555,196 |
| pinned, 212,992 (a stock host's clamp) | 425,984 | 319,385 |

The cell's 451,584 is 0.86 × 524,288 and the runner's 395,000 is 0.75 ×
524,288; the ratio is the kernel's learned payload fraction of an skb,
which differs between a real interface and a netem loopback. Both say the
same thing: after a post-connect pin the window is capped at the pinned
buffer, thirteen times below what autotuning reaches on this host, and it
is not frozen near the 64,088-byte establishment clamp. So of the four
possibilities, it is the fourth in the harmless form: my reading was
right and my sentence was ambiguous, and no two readings of this kernel
disagree.

What is kernel-dependent is the magnitude of the loss, not its sign. The
reporter measured, with `strace` and `ss` on their production host, a
window that stayed near 64 KB; on that kernel the clamp is left at its
establishment value by a post-connect pin, and the loss is the whole
window. On `7.0.12` the clamp follows the pin, and the loss is the gap
between the pinned buffer and autotuning. On a stock host both are a
loss of one order of magnitude or more against `tcp_rmem[2]`, and the
deletion wins on both; on a host whose `2 × min(request, rmem_max)`
exceeds `tcp_rmem[2]`, the pin wins on `7.0.12` and still loses on the
reporter's kernel. §15's rule is right on both because it applies a
receive pin only before connect, which sets the clamp from the buffer on
every generation. Two things to tell the reporter: their diagnosis holds
and is worse on their kernel than on a current one, and their fix is
conditional twice, on the budget inequality of 15.1 and, for any future
pin, on the kernel's treatment of a post-connect request.

### 10.11 What 41d5045 does and does not answer, stated after the measurement stream's reading

The measurement stream predicts, from the source, that a client whose
acknowledgements are withheld past the timeout and then resumed is
released on both trees. That is correct, and it is the trade §10.3 states:
silence runs from the latest of the last acknowledgement, the outstanding
count last rising from zero, and the last carrier absence, and a client
that acknowledges nothing advances none of them. 41d5045 does not change
that outcome and was not built to. What it changes is the quantity, which
removes three false positives main has and this tree does not: a client
that acknowledges slowly with several flows parked (row A2, §17's second
shape), a freed slot restarting the clock (row A3), and the provider's
own carrier being down (row A4). H4 as first posed, a live client that
goes completely silent behind a stall and comes back, is a different
problem and remains a decision on an estimate.

Whether the provider can tell that client from a departed one: from its
return path, no. Total silence is total silence, and the wedges of 209,
264 and 723 s were exactly that on the relay route. From outside the
return path there are two facts, one available now and one not:

- A live direct route. When the destination is reached over a P2P
  transport, that transport's own liveness (its consent and heartbeat
  cadence) is the client's liveness within the transport's detection
  time, and silence on the return path while the direct route stays up
  is inadmissible. The route manager has no per-destination predicate
  today, only `HasActiveTransport()` for the whole client; the addition
  is `RouteManager.HasActiveDirectRoute(destinationId)`, reading the
  writer match state's routes for a transport bound to that peer, and
  `abandonSilentSource` advancing `carrierAbsentNanos` while it is true,
  the way it does for the carrier. Not built in this round; it is the
  next change in this path and it converts the trade into a decision on
  a fact for every P2P-connected client.
- Platform presence for exchange-relayed clients, §10.8, which is the
  route the wedges were measured on and the only fact for it.

The trade for the exchange-relayed case, and its cost, stated for the
landing decision: a client silent for 120 s while the provider holds a
carrier has its NAT flows retired and its return sequences cancelled, is
readmitted at once, and on return finds its inner connections reset and
re-establishes them; the retained bytes of the parked returns are lost
with the flows. Per event that is one reconnect for that user. The
benefit it buys is the reporter's: a departed client's zombies are gone
in 120 to 150 s instead of never, which the report measured at 72 per
cent of a provider's throughput at 40 of them. The rate of such events in
production is not known; a counter of releases per provider
(`releaseUnreachableSource` fires once per event and should increment a
`CongestionDropStats` field, a one-line follow-up) would make it known,
and the previous program's 0 of 120 runs silent for 120 s on the shipped
tree is the only measurement in hand.

The discriminating cell the stream describes, acknowledgements that
continue but throttled so individual items park for many timeouts, is the
right one for what was built. The evidence keys on the Transfer-level
acknowledgement of this source's socket-owned return packs (each
`sendAckResult` on the source's `sourceAckEvidence`) and on the count of
those packs outstanding; it does not key on inner TCP acknowledgements,
on packets received from the client, or on any other source. So the cell
needs some acknowledgement of the source's returns to land at least once
per 120 s while individual items wait longer than 120 s for a slot: forty
flows on a client shaped to 50 kb/s gives a mean wait of forty times
24 KiB over 50 kb/s, about 157 s. Main releases that client; this tree
does not. Its positive control is the same client with all
acknowledgements blocked for 150 s, which releases on both.

| Row | Test | Pins | Fails on | Regime |
|---|---|---|---|---|
| A11 | `TestLiveClientSilentPastTheTimeoutIsReleasedAndReadmitted` | the trade, explicitly: acknowledgements withheld past T while the provider holds a carrier; the release fires within [T, 1.3 T] of the last acknowledgement; the parked producer returns with sent = false; the source is readmitted; a subsequent return from the same source is admitted under a fresh lifecycle whose evidence reads nothing outstanding | holds on both trees; documents the trade and keeps it from being widened silently | in-process |

### 16.5 What the 32 MiB budget scales, and the tunnel-side prediction

The retrospective incidence, 5 of 10 at 32 MiB, 0 of 91 at the default
and 1 of 63 at 1 MiB, points at what that budget feeds. From the
constructors, the settings that differ, with the 1 MiB and default
values beside them:

| Setting | 1 MiB | 32 MiB | default |
|---|---:|---:|---:|
| `TcpBufferSettings.ReadBufferByteCount` (socket read) | 16 KiB | 32 KiB | 64 KiB |
| `TcpBufferSettings.InitialWindowSize` (NAT window toward the client) | 128 KiB | 512 KiB | 1 MiB |
| `TcpBufferSettings.MaxWindowSize` | 256 KiB | 8 MiB | 16 MiB |
| `TcpBufferSettings.SequenceBufferSize` | 192 | 512 | 1,024 |
| `SendBufferSettings.ResendQueueMaxByteCount` (return sequence, per lane) | 256 KiB | 1 MiB | 2 MiB |
| `ReceiveBufferSettings.ReceiveQueueMaxByteCount` | 320 KiB | 1.25 MiB | 2.5 MiB |
| `TcpBufferSettings.GlobalLimit` (flows) | 64 | 256 | unlimited |

A return sequence can hold `ResendQueueMaxByteCount` unacknowledged, so
one client's download through the provider is bounded by that over the
acknowledgement round trip. At 32 MiB that is 1 MiB per acknowledgement
RTT, and 16 Mb/s is 1 MiB per 520 ms. So the prediction, before the
40-repetition sweep: in the stuck runs the return sequence to the client
sits at its 1 MiB bound with an acknowledgement round trip near 500 ms
(the queue-inflated state the flight-gate program called M4), and the
upstream window sits at 104,448 because the reader is draining only what
the tunnel takes; in the engaged runs the queue is under its bound and
the round trip near 50 ms. The readings are `Client.ResendQueueSize` for
that destination and the sequence's `ReliableLaneLongestAckGap` and
round-trip window, sampled through the run, beside `ss -ti rtt` on the
upstream socket. Why the inflation would be bistable and specific to
1 MiB is the part the sweep must show; the candidates are the exchange
forward buffer and the client's tun receive path, and 104,448 is the
kernel's window from a 131,072-byte buffer at its learned payload ratio,
not a provider constant.

### 18.1 The single-flow ceiling already fits the reliable bound

The measured single TCP flow, 0.26 to 0.33 Gb/s, is 2 MiB per 50 to
65 ms: `ResendQueueMaxByteCount` over the tunnel's acknowledgement round
trip, §18's second item, with no other term needed. The cheapest
experiment in the program follows: raise `ResendQueueMaxByteCount` to
8 MiB in the provider's settings and repeat the single-flow cell. If the
flow rises by about four times toward the UDP figure, the ceiling is the
per-destination reliable bound and the design question becomes how to
size it, by the acknowledgement round trip and the measured delivery
rate rather than by a fixed byte count, or by lanes; if it does not, the
reader's synchronous admission, §18's first item, is next. Sixteen flows
at 0.59 to 0.80 Gb/s then say whether they shared a client: sixteen to
one client cannot pass the same bound, so either they spanned clients or
their round trip differed, and the cell should record which.

## 19. The two directions from source: what blocks, what waits, and where bytes funnel

Read from `TcpSequence.Run` and `LocalUserNat`, 2026-09-13, against the
measurement stream's cell (a gVisor `Tun` and a `LocalUserNat`, nothing
above them) and confirmed from outside by the direction asymmetry it
measured: uploads at 420 to 470 Mb/s at every budget, downloads at 100 to
300, and a sixteenfold sweep of the acknowledgement compression timeout
that moves upload from 673 to 5.5 Mb/s and download not at all.

### 19.1 Upload, client to origin: the ladder, the compression, the blocking signal

A segment from the client arrives on the flow's `sendItems` channel and
is handled by the sequence goroutine in `handleSendItem`. The NAT's view
of the client's stream is `sendSeq`, the next byte expected from the
client, which is also the acknowledgement number the NAT writes toward
the client. The payload is offered to `writePayloads`, a channel of
`SequenceBufferSize` payloads (1,024 unbudgeted, 512 at 32 MiB, 192 at
1 MiB, counted in payloads and not bytes), and the socket writer
goroutine drains it in vectored writes of up to `WriteBatchSize` (64)
payloads into the upstream kernel socket with a progress deadline of
`WriteTimeout`.

The blocking signal is exact: the offer is a `select` with a `default`
branch; if the channel accepts at once the payload counts as
`nonBlockingByteCount`, otherwise the goroutine waits on the channel and
the payload counts as `blockingByteCount`. The channel is full when the
socket writer is behind, and the socket writer is behind when the kernel
send buffer is full, which is when the origin's window or the congestion
window on the provider-to-origin path is full. So the signal reports the
upstream path's absorption rate, one queue removed.

The ladder acts on `windowSize`, the window the NAT advertises to the
client in every segment it sends (`encodedWindowSize`), floored at
`MinWindowSize` (64 KiB), starting at `InitialWindowSize` (1 MiB
unbudgeted) and capped at `MaxWindowSize` (16 MiB unbudgeted). Once
`windowSize` bytes have been offered since the last evaluation it doubles
if all of them were non-blocking, halves if at least half were blocking,
and otherwise stays, resetting both counters each time. It is an
equilibrium seeker, as the correction to the brief says, and it reports
where the writer first pushes back; it does not seek the cap. The
measurement stream's fit of a six megabyte window under a saturated
upload says it climbs several rungs, which the rung histogram it is
sampling can confirm.

The acknowledgement goroutine sends a pure ACK whenever `sendSeq` has
advanced past `ackedSendSeq`, then waits for the earlier of
`AckCompressTimeout` (50 ms) and `ackSignal`, which the sequence goroutine
raises once unacknowledged client bytes reach half of `windowSize`. A data
segment emitted toward the client by the reader also carries the current
acknowledgement (`ackedSendSeq = sendSeq` there), so a bidirectional flow
acknowledges for free. On a pure upload the interval between ACKs is
`min(T, W/(2R))` for timeout T, window W and rate R, and the timer binds
below `R = W/(2T)`, which at W = 6 MB and T = 50 ms is 500 Mb/s: at every
measured rate the timer is the trigger, which is why the sweep moves
upload so cleanly.

### 19.2 Download, origin to client: the client's window, the acknowledgement condition

The socket reader goroutine reads up to `ReadBufferByteCount` from the
upstream socket. Under the connection mutex it computes
`receiveWindowSize − (receiveSeq − receiveSeqAck)`: `receiveSeq` is the
NAT's own sequence number toward the client, `receiveSeqAck` the highest
the client has acknowledged, and `receiveWindowSize` the window the
client last advertised, parsed from the SYN (`tcp.windowSize` shifted by
`receiveWindowScale`) and updated from every client segment in
`applySendAckWithLock`. If room exists it packetizes `min(room, read)`
bytes into MTU-sized segments (`DataPackets`, one pool copy per segment)
and advances `receiveSeq`; if none, it waits on `receiveAckCond` until a
client ACK, applied on the sequence goroutine, broadcasts. Segments go to
`readPackets` (64 deep), the batch consumer drains them and calls the
receive callback, and the callback is the device's tun write in the cell,
or the provider's synchronous Transfer admission in production.

So the download is bound by the client's advertised window over the time
a client acknowledgement takes to come back through the tun, the NAT's
single ingress dispatch shard and the sequence goroutine, and by nothing
the provider configures: no ladder, no compression timer, no
`MaxWindowSize` (which only sizes the reorder bound and pools here). A
pure download advances `sendSeq` only by pure ACKs, which carry no
sequence space and return from `handleSendItem` at once, so the
compression timer never even arms. This is why the sweep left download at
265 to 312 across sixteenfold, and the direction is settled from source
and from measurement alike.

In the cell the client's window is ours, not gVisor's: `tun.go` sets
`TCPReceiveBufferSizeRangeOption` and `TCPSendBufferSizeRangeOption` from
`TunSettings.TcpReceiveBuffer` and `TcpSendBuffer`, whose defaults are
memory-scaled, default 1 MiB and maximum `MemoryScaledByteCount(4 MiB,
512 KiB)`. An in-process cell scales the client's windows with the
provider's budget, so a 1 MiB budget gives the gVisor client a 512 KiB
maximum window and a 32 MiB budget a 2 MiB one; the harness's client
binds first at small budgets by construction. Whether the download's
ceiling at the default budget (about 300 Mb/s against a 4 MiB client
window) is that window over its acknowledgement turnaround or gVisor's
per-byte cost is the open question the coordinator's sweep is settling;
19.2's arithmetic says a 4 MiB window at 300 Mb/s needs a 110 ms
turnaround, which the in-process path does not have, so the window is
not the likely binder and per-byte cost in the harness client is. The
reading that decides is the same as §16's: the reader's time in
`receiveAckCond.Wait()` per flow.

### 19.3 The serialization map of the download path

From the upstream socket to the device, every point where bytes of one
flow, or of all flows, pass one goroutine, channel or mutex, with its
scope:

| Point | What passes it | Scope |
|---|---|---|
| socket reader goroutine: read, window check, `DataPackets` copies | every byte of one flow | per flow |
| `readPackets` channel (64 packets) and its batch consumer goroutine | every packet of one flow | per flow |
| the receive callback, synchronous on the batch consumer | every batch of one flow | per flow, but see below |
| in the cell: the tun write into gVisor (`InjectInbound`, stack dispatch, the endpoint's mutex, ACK generation) | every packet of every flow on the device | per device; per endpoint inside gVisor |
| in production: `enqueueReturnItem` and the synchronous `sendReturnItem` on the flow goroutine | every batch of one flow | per flow |
| the Transfer send sequence to the client: `packs` channel, `Run` goroutine, contract accounting, session encryption, framing, the resend queue | every pack to one client, all of its flows | per destination (`sendSequenceId`, §20) |
| the multi-route writer and the transport connection's writer (H1: TLS records; H3: QUIC) | every pack to every destination of this provider | per provider client, per carrier family |
| the exchange: resident ingress shards, then one forward per destination | every frame from this provider | per provider at ingress, per destination at the forward |
| the client's transport reader and its receive sequence per source (ordered delivery, decryption) | every pack from this provider | per source at the client |
| the client's `LocalUserNat` and tun write | every packet on the device | per device |

The return direction of the download, the client's ACKs, funnels through
`LocalUserNat`'s ingress dispatch, `SendShardCount` 1 by default, a
single goroutine hashing every packet from the device to its flow's
`sendItems` channel: per NAT, and shared by every flow's ACKs; at tens of
thousands of small packets a second it is not the binder, but it is the
one truly unsharded point below Transfer and worth a counter.

Sixteen flows in the cell share the device's gVisor stack and the NAT's
single ingress shard; in production they also share the per-destination
sequence and the per-client transport. The cell's 2.5x from one flow to
sixteen is what parallel per-flow work above shared per-device work looks
like; the reporter's 1.0x from one flow to eight, with the upstream socket
removed, is what a per-destination serialization looks like, which is
§20.

## 20. One sequence per client: the key, what it serializes, what lanes would change

### 20.1 The key, from source

`sendSequenceId` is `Destination`, `CompanionContract`, `ForceStream`,
`LogicalLane`, `EncryptionRole` and `EncryptionCompanion`
(`transfer.go`). No flow, no five-tuple. Ordinary provider return traffic
uses one transfer key per source (`providerReplyTransferKey`), so every
IP flow the provider returns to one client rides one send sequence: one
`Run` goroutine, one `packs` channel, one resend queue, one ordered
sequence-number space, one contract at a time. `LogicalDataLaneCount` is
0 as shipped, so `LogicalLane` is 0 for all of it; the setting's comment
says it waits on a one, four and eight lane campaign, and that receivers
already understand and advertise bounded lanes. Confirmed: the
per-client serialization the reporter's data points at is real, the
escape is built and off, and it is wire-compatible.

The resend queue bound is `ResendQueueMaxByteCount` per sequence, 2 MiB
unbudgeted (1 MiB at 32 MiB, 256 KiB at 1 MiB); on an sdk-hosted
provider all sequences also share `ResendQueueBudget`. TCP returns are
`Ack` packs and occupy it; UDP returns are NoAck and bypass it but still
pass the same goroutine. So the bound is per client, not per flow.

### 20.2 What the single sequence costs a download

Ordering. The receive side delivers in sequence-number order
(`ReceiveSequence.nextSequenceNumber`; an item above the head is queued,
bounded by `ReceiveQueueMaxByteCount`, and delivered only when the hole
fills). A pack lost or delayed on the way to the client therefore holds
every later pack, of every flow of that client, until it is recovered:
head-of-line blocking across unrelated TCP connections, real, and
nothing downstream reorders, because the client's NAT and tun receive
packets in delivery order and each inner TCP connection sees its own
segments in order only because the whole stream is. One recovery
interval at the sender's timer, 300 ms to 8 s, stalls the client's every
flow.

Per byte in the sequence goroutine: contract accounting on the head,
`setHead` and framing, the session cipher's AEAD over each pack
(`writeMaybeWrappedBytes`), the multi-route write into the transport,
plus the resend-queue bookkeeping and the acknowledgement window; then,
on the transport writer goroutine, the carrier's own encryption again
(TLS records on H1, QUIC on H3). Two encryptions and one goroutine
handoff per pack, on a single goroutine per client. That is a serialized
handoff whose service time sets the rate for that client, at any number
of flows, at low CPU, which is the shape the reporter measured: 664.5 on
one flow, 677.0 on eight, 671.0 with the upstream socket removed
entirely, and nothing saturated.

The per-destination reliable bound at their round trip: 2 MiB over 2 ms
is 8.4 Gb/s and does not bind; over 20 ms it is 840 Mb/s and would sit
just above their ceiling. Their round trip is not in the report; if it is
the datacenter's few milliseconds, the queue is not the mechanism and the
serialized per-pack work is, and §18's second item is dead for their path
as well. `Client.ResendQueueSize` for the client during a run says which:
at the bound, the queue; below it, the goroutine.

### 20.3 What lanes would cost, argued before the number

Memory. Nonzero lanes share one lazily built `logicalLaneResendBudget` of
`ResendQueueMaxByteCount` (the comment: allocation-neutral for disabled
and legacy clients, one pool shared by every nonzero lane), so the resend
budget per client does not multiply with the lane count; `SequenceBufferSize`
is divided among lanes (`logicalLaneSequenceBufferSize`), so the pack
channels do not multiply either. What does multiply is per-sequence
fixed state, a few KiB each (ack window, RTT window, lane acks, contract
bookkeeping, goroutine stack), and contracts: each sequence holds its own
contract, so N lanes are N contract requests per client and N escrows,
which is platform load and control round trips rather than device
memory. The program's memory ceiling is not the obstacle; contract fan-out
is the cost to weigh.

Ordering. Lane zero today gives one total order over everything to a
destination: data of all flows, synthesized controls, encryption
handshake, contract frames. Five-tuple-hashed lanes give order per
tuple, with lane zero keeping control and anything unhashable. Nothing an
IP path promises is lost: TCP needs order per connection, which a lane
preserves. What changes and must be checked before a campaign: IPv4
fragments after the first carry no ports, so a tuple hash sends them to
a different lane than the first fragment and reassembly at the client
must tolerate arrival order (the reassembler is keyed by identification
and should; row L3 pins it); ICMP errors about a TCP flow hash on the
ICMP tuple and may arrive before or after the data they concern, which is
harmless; and a flow's synthesized RST or SYN-ACK shares the flow's
tuple and lane, so its order relative to the flow's data holds. I find
nothing in connect or the sdk that relies on cross-flow order to one
destination; the receiver's per-lane sequences exist precisely so that
it does not.

What lanes do not change: the transport connection per carrier family is
still one writer per provider, and the client's tun is still one device.
Lanes parallelize the sequence goroutine's work and remove cross-flow
head-of-line blocking; the next serialization after them is the carrier.

| Row | Test | Pins | Fails on | Regime |
|---|---|---|---|---|
| L1 | `TestProviderReturnsToOneClientShareOneSequence` | with lanes off, TCP flows to one destination produce one `sendSequenceId`; `SequenceCount` in `DestinationSendStats` reads 1 | none; characterisation | in-process |
| L2 | `TestLostPackHoldsEveryFlowOfTheClient` | with lanes off, dropping one pack of flow A delays delivery of flow B's later packs until A's recovery; with eight lanes, flow B's packs on another lane are delivered meanwhile | lanes off, by construction; documents the cost | in-process, two lanes at least |
| L3 | `TestFragmentsReassembleAcrossLanes` | an IPv4 datagram fragmented at the provider and hashed to two lanes reassembles at the client in either arrival order | a reassembler that assumes order | in-process |

## 21. The fourth send cell: the classification predicts the buffer, the path decides the effect

The rule's four predictions against the campaign: gain at 1 MiB
(measured +363 to +403), null at 8 MiB (measured null), pin wins at
32 MiB (measured −20.8 for the deletion), pin wins unbudgeted (measured
−11.5 inside a ±43 null band). The runner's values, read rather than
inferred: `wmem_max = 4,194,304`, `tcp_wmem[2] = 4,194,304`. So at 32 MiB
the request is 8 MiB and obtains 8,388,608; unbudgeted the request is
16 MiB and obtains the same 8,388,608. The two cells pin the identical
kernel buffer, and the classification is right that in both the pin is
larger than autotuning's 4 MiB.

What the classification does not say is whether the buffer binds. The
buffer binds when `buffer × 8 / RTT` is below the rate the rest of the
path sustains, and the compression sweep gives that rate: about 465 Mb/s
at the shipping 50 ms. An autotuned 4 MiB over 50 ms carries 671, above
465, so unbudgeted neither arm's buffer binds and the null is what the
physics predicts; at 1 MiB the pinned 524,288 carries 84, far below, and
the deletion is the whole gain; at 8 MiB the pin equals the ceiling and
it is a wash. The coordinator's chain is right and the runner's values
confirm both ends of it.

The 32 MiB cell is then the odd one: the same 8 MiB pin against the same
4 MiB autotuning ceiling, above the same 465, and yet the deletion cost
20.8 per cent. Two readings. Either the autotuned arm at 32 MiB does not
reach 4 MiB, because autotuning sizes the send buffer to the congestion
window and the offered load shape at that budget (a 512-deep
`writePayloads`, a 512 KiB initial window, an 8 MiB cap) earns a smaller
window, so its buffer sat at two to three megabytes and did bind; or the
−20.8 at five repetitions is the reading to doubt. `ss -tmi` on the
autotuned arm's upstream socket at both budgets decides the first
(`skmem tb` at 32 MiB below 4 MiB and at the default at 4 MiB), and the
coordinator's 12 ms pair decides the second: with the ack-limited rate
raised to about 673, above the 671 an autotuned buffer carries, the pin
should reappear as a gain at the default budget.

The rule as built does not depend on the resolution. It is main's pin
everywhere main's pin exceeds the ceiling, and the kernel elsewhere, so
against main it ties at 8 MiB, 32 MiB and the default and wins at 1 MiB;
it cannot regress main in any of the four cells whatever the missing
term is. What the missing term changes is the claim in 15.2 that the
32 MiB loss "is recovered": that is true only if the loss is real, and
the pair above says whether it is. The restated rule for the design
record: the classification decides which arm has the larger buffer,
exactly; pinning matters only where that buffer is below what the path
sustains; above it both arms are limited elsewhere and the comparison is
a null whichever buffer is larger.

## 22. The acknowledgement compression: what it buys, what bounds it, and the floor it must stay under

### 22.1 The model, checked against the loop

The coordinator's model is the loop of 19.1: on a window-limited upload
the compression adds to the effective round trip, `rate ≈ W / (RTT + T)`
while the timer is the binding trigger, which it is below `W/(2T)`. With
the sweep's fit (W about 6 MB, base turnaround about 63 ms) it predicts
574 at 25 ms against 603 measured and 447 at 50 against 465, and the
shape including the sub-linear fall-off. I have no better fit from source.
The half-window trigger is the rate-dependent half of the setting and
already exists; the timer is the idle bound, and it is what binds at every
rate the fleet sees.

### 22.2 What it costs to lower, in packets and in wakeups

Packets: with the timer binding, the ACK rate is 1/T per sequence: 20 per
second at 50 ms, 83 at 12 ms, about 5 KB/s of 60-byte packets. Against a
saturated upload's thousands of data packets per second that is noise,
and the gain on the cell's path is about 45 per cent, more on faster
paths since T dominates the sum. So on packets and bytes the constant is
buying almost nothing.

Wakeups: on a phone the radio and the CPU wake per packet, and a rate
limit on acknowledgements protects against wakeups that a packet count
does not show. But the compression only matters while the client is
uploading, and an uploading client's radio is awake for its own data
packets at a far higher rate than 83 per second; a trickle upload that
would otherwise be idle produces one ACK per burst under either constant.
I find no wakeup regime in which 12 ms costs what 50 ms saves, and no
comment in the source stating the reason 50 was chosen; the setting's
comment says only that the half-window signal keeps the source from
stalling on the timer.

### 22.3 The bound from above, and what it must not cross

A sender whose acknowledgement is held longer than its retransmission
timeout floor retransmits data that was not lost, and each spurious
retransmission halves its window; the collapse to 5.5 Mb/s at 200 ms is
that cliff. The floor on the sender's side is the peer's stack's minimum
RTO: 200 ms in gVisor (`MinRTO`) and in Linux (`TCP_RTO_MIN`), and the
effective RTO is the smoothed round trip plus four deviations, so a
compression jitter of 0 to T inflates the deviation term and moves the
effective timeout up with T; the floor is what binds when the path is
short. The shipping 50 ms has a fourfold margin to a cliff that costs 98
per cent of upload, against a constant in a vendored dependency that
nothing in this repository asserts.

Now that the tun exposes its floor (`TunSettings.TcpMinRto`, landed
beside `TcpMaxRto`), the relationship can be stated in code rather than
carried in a margin. Of the three shapes: a runtime guard would couple a
NAT setting to a device-stack setting that lives in a different process
on every real deployment, since the peer is the client's stack and not
ours, so it cannot enforce what it claims; a comment cannot fail. A test
assertion is the right shape: `TestAckCompressionStaysUnderTheRetransmissionFloor`
pins `DefaultTcpBufferSettings().AckCompressTimeout` at no more than one
quarter of the gVisor stack's default minimum RTO read from the vendored
constant, and no more than one quarter of any `TcpMinRto` the tun ships
with, so a change to either constant fails a test that names the cliff.
Row C1 below.

### 22.4 The shape of the setting

Right as it is: a timer for the idle bound plus a half-window trigger for
the rate-dependent bound is the pair a receiver needs, and a rate-
dependent rule would recompute what the half-window signal already
supplies. What is wrong is the constant's position on the interval
between what compression protects (nothing found, 22.2) and the floor it
must stay under (22.3): the shipping value sits at the top of that
interval, where it costs most and protects least. The measured curve
between 12 and 50 ms is the design input a campaign picks from; this
round picks nothing.

| Row | Test | Pins | Fails on | Regime |
|---|---|---|---|---|
| C1 | `TestAckCompressionStaysUnderTheRetransmissionFloor` | the default `AckCompressTimeout` is at most a quarter of gVisor's default minimum RTO and of `DefaultTunSettings().TcpMinRto` when set | a constant moved past the cliff in either place | pure |
| C2 | `TestHalfWindowSignalFiresBeforeTheTimerAboveTheCrossover` | with W and T chosen so that `W/(2T)` is below the offered rate, ACKs are paced by the half-window signal and their interval is under T; below it, by the timer | none; characterises the trigger | in-process |

### 20.4 The memory objection, verified answered on both sides; ordering is the question

Read from `newSendSequenceWithLogicalLane` and the receive buffer's
sequence construction. For a nonzero lane the per-sequence floor is
zeroed (`resendQueueMinByteCount = 0`), so lanes claim no guaranteed
floors; when the caller supplied a device-wide `ResendQueueBudget` (every
sdk-hosted provider does, `configureDeviceLocalProviderMemory`) the
lanes draw from it and nothing multiplies; when none was supplied (the
bare provider) all nonzero lanes share one lazily built pool of one
`ResendQueueMaxByteCount`. The receive side mirrors it exactly: one
shared pool of `ReceiveQueueMaxByteCount` for every data lane when no
`ReceiveQueueBudget` was supplied, the device budget otherwise. So eight
lanes on a bare provider cost lane zero's queue plus one shared pool,
about 4 MiB at the shipping bound, and on a phone receiving a download
on eight lanes the cost is the fixed per-sequence state, a few KiB each.
The direction asymmetry stands: the sequences that carry a download live
on the provider, and a phone pays for lanes only on what it sends and
receives, at pooled cost. Contract fan-out (20.3) remains the one cost
that multiplies, and it lands on the platform rather than on a device.

The feature's shape says it was finished to this point on purpose:
pooled budgets on both sides, receivers advertising support so senders
roll out alone, the count left at zero for a campaign. The ordering
analysis of 20.3 is therefore the whole of what a lane rollout can get
wrong, and rows L2 and L3 are the ones to run before the one, four and
eight lane campaign reads a number.

### 20.5 What a zero floor means for a lane under contention

The floor is not a reservation the pool holds back for a lane; it is the
part of a sequence's queue that is admitted without consulting the pool
at all. `transferQueue.CanAddWithQueueByteCount` admits when
`borrowTarget − borrowed ≤ budget.Available()` with
`borrowTarget = max(0, queued − minByteCount)`: the first `minByteCount`
bytes of a queue are the sequence's own, everything above them is
borrowed from the shared pool. Lane zero, and every sequence to a
distinct destination on an sdk-hosted provider, keeps
`ResendQueueMinByteCount` (256 KiB) that way; a nonzero lane has
`minByteCount = 0`, so every byte it holds is borrowed.

Under contention that is exactly the coordinator's shape. The shared
lane pool is one `ResendQueueMaxByteCount`, and each lane's own cap is
the same number, so one bulk flow's lane can hold the entire pool. The
other lanes are not deadlocked, because `CanAdd` always admits one item
into an empty queue, so a light lane keeps one Pack in flight; that is
its floor in practice, one Pack per acknowledgement round trip, 24 KiB
over 50 ms is 3.9 Mb/s, against the whole pool for the heavy lane. When
the heavy lane releases bytes the pool notifies every waiting sequence
(`CapacityNotify` is a broadcast; the FIFO grant list in
`notifyEligibleCapacityWaiters` that scans past a large request so a
smaller one cannot starve is used by the WebRTC managers, not by
sequences), and the sequence whose goroutine runs first borrows them;
the heavy lane has a Pack ready more often, so it wins more often. Lane
zero today has neither the head-of-line cost nor this one: a light
flow's Pack waits its FIFO turn in the single queue and is never held to
one in flight. So lanes as built trade cross-flow head-of-line blocking
for cross-flow unfairness that appears only under load and only with a
heavy flow, which a clean campaign would not reach and which would not
generalise past what it measured. The code prevents the deadlock and
nothing else.

What prevents it, and the design already contains the idea: the same
floor semantics the sequences to different destinations have. A data
lane with `minByteCount` of the pool divided by the lane count keeps its
first share without borrowing, and the pool then needs to be sized as
the sum of floors plus one cap so that borrowing still exists: for eight
lanes at the shipping bound, seven floors of 256 KiB plus 2 MiB is under
4 MiB per client on the bare provider, which is where the download
sequences live, and on an sdk-hosted provider the device budget already
carries the floors for every sequence. The alternative, sequences
taking their grants from the FIFO list instead of the broadcast, gives
fairness without reserved bytes but changes an admission path that every
sequence shares and is the larger change. Either is a design decision
the campaign must precede with row L4, because without it a lane count
chosen on clean cells ships the unfairness.

| Row | Test | Pins | Fails on | Regime |
|---|---|---|---|---|
| L4 | `TestLightLaneKeepsItsShareBesideASaturatingLane` | eight lanes, one bulk flow saturating its lane and one light flow on another; the light flow's Packs in flight stay above one and its delivery rate stays within a bound of its lane-zero rate | lanes as built, by 20.5; holds once floors or fair grants exist | in-process, contended |

## 23. The race suite's one failure per run: the tests, not the shipped code

Two full runs on the final tree, 1,457 and 1,456 s, zero data-race
reports, every subpackage green, and in each run one different row
failed under full load and passed three of three alone. The judgement
the coordinator asked for, from the rows' own output.

`TestWebRtcFastPathFitsIpv6MinimumMtuOnActualWire` failed at
`WaitFastPathReady(pair.ctx, 10*time.Second)`: a fixed ten-second
deadline on ICE, DTLS and SCTP establishment over pion's virtual network,
before the behaviour under test, whether the fast path fits the IPv6
minimum MTU on the wire, is exercised at all. Under the race detector on
a saturated host the establishment took longer than ten seconds and the
row failed on setup. That is the test asserting on elapsed time for a
step it does not test; the code under test was never reached. Fix: bound
the readiness wait by the test's own deadline (`t.Deadline`, the package
timeout) and assert that readiness arrives, not when.

`TestReceiverBudgetDropsDoNotWedgeEitherArm` failed at its comparability
guard: "the receiver dropped nothing on one arm". Its output reads
`rule off: 1.334s, 0 dropped` and `rule on: 18.727s, 13 dropped`. The
shipped configuration (the lane rule off) completed in 1.3 seconds and
the receiver dropped nothing, which is the receiver keeping up under a
schedule that let it; the row needs the receiver's budget to overflow to
reach the shape §34.2 names, and it induces that by sending faster than
the receiver drains, which is scheduling luck under load and exactly
what CODESTYLE's test rule says a proof must not rest on. The eighteen
seconds on the rule-on arm are the known behaviour of the mechanism that
ships off (§36.10), and the row's own comment records that this arm
exceeded a flat twenty seconds at 20.07 s inside the whole suite before
the bound was made derived. Fix: force the overflow with a hook or
barrier, holding delivery until the queue is full and then releasing, so
the drop is a fact of the fixture rather than of the schedule.

So: the tests are timing-sensitive under contention, in setup and in
fixture shape, and the shipped code is not shown to be. What would
distinguish the other case is simple and neither row needed it: rerun
under the same load with the setup deadline derived and the drop forced;
a transfer that completes is a test problem, a transfer that stops is a
code problem, and the shipped arm here completed in 1.3 seconds. The
follow-up is the two fixture changes above, named so the habit of
re-running does not set in.

## 24. The ladder's dynamics: an equilibrium band, a backlog collapse, and a fast climb

From the two rules in `handleSendItem` (§19.1): an evaluation happens
once `blocking + nonBlocking ≥ W` bytes have been offered; it doubles
only if `nonBlocking ≥ W`, which at that instant means no byte blocked;
it halves if `blocking ≥ W/2`; otherwise it holds; both counters reset.
A payload blocks when `writePayloads` (`SequenceBufferSize` payloads,
1,024 unbudgeted, about 1.4 MB at the client's segment size) is full at
the offer, which is when the socket writer is behind the offers.

Steady state, with the client sending at its window W per round trip
and the writer draining at the origin path's rate D: the channel fills
only if `W/RTT > D`, and then the fraction of bytes that block is about
`1 − D·RTT/W`. Halving needs that fraction at or above one half, that is
`W ≥ 2·D·RTT`; doubling needs it at zero, that is `W < D·RTT`. Between
the two the window holds. So the ladder settles in the band
`[D·RTT, 2·D·RTT)`: one to two bandwidth-delay products of the origin
path, which is the equilibrium reading of the earlier brief, and the
measurement stream's six megabyte fit at 50 ms puts D near 480 to
960 Mb/s, bracketing the measured upload. No runaway in this regime.

The collapse regime is a backlog, not a rate. While the writer is fully
stalled the send loop is parked inside a blocking offer and the counters
do not advance, so nothing halves during the stall itself. When the
writer resumes, every parked and queued byte completes as a blocking
byte, and a backlog of B bytes supplies B/2 halvings' worth in a row:
the first evaluation needs W/2 blocked bytes, the next W/4, and the sum
of the whole descent to the 64 KiB floor is about W. So a backlog of at
least one window, which a stall of one round trip on a saturated upload
produces (the channel plus the client's in-flight data), can drive the
ladder from its equilibrium to the floor in one drain. This is a
feedback in the sense the brief asks about: each halving shortens the
window the next halving needs, so the descent accelerates as it goes,
and a burst of blocking of roughly one window is enough to reach the
bottom. The same arithmetic applies to a bursty client: a source whose
acknowledgements arrive every T sends its window as one burst, and if
the burst exceeds what the channel holds plus what the writer drains
during it, half of it blocks and the window halves, every burst, until
the burst fits. That is a candidate for the 200 ms cliff whose
retransmission explanation died, and the rung readout decides it: if
the advertised window at 200 ms sits at the floor or one rung above,
the ladder collapsed under burst blocking; if the window is still large
while the implied in-flight is 128 KiB, the collapse is the client's
congestion window, not ours.

Recovery: a climb needs one evaluation window with no blocking at all,
and at the floor that is 64 KiB of client data with the channel already
drained, which a working writer absorbs without a single block; each
doubling then needs about one round trip of client data, so the climb
from 64 KiB to a six megabyte equilibrium is about seven evaluations,
roughly seven round trips, 350 ms at 50 ms. A transient, not a latch,
with two conditions: the backlog must have drained first, since any
residual blocked byte holds the window, and the client must be sending,
since an idle client's ladder stays where it fell until its next data,
which is harmless because it climbs in the first seven round trips of
the next transfer. The one way it latches is a writer that stays behind,
and then the floor is the honest reading of an origin path that cannot
absorb more.

What the collapse mis-measures is the thing to name, without a remedy:
bytes that block because a backlog is draining are counted as if the
current window exceeded the path, so a single stall is charged as many
windows' worth of evidence. A production provider whose upload ladder
has just collapsed advertises 64 KiB to that client for the next several
round trips, about 10 Mb/s on a 50 ms path, which is the same order and
nearly the same number as the reporter's original download symptom, on
the opposite side of the provider and in the opposite direction; they
share nothing but a 64 KB window on a 50 ms path, and should not be
conflated. Rows for the record: `TestUploadWindowSettlesBetweenOneAndTwoBdps`
(pure, a modelled writer at rate D), `TestBacklogDrainCollapsesTheWindowToTheFloor`
(a writer held for one round trip then released, the window descends
to `MinWindowSize` within that drain) and
`TestCollapsedWindowClimbsInLogRoundTrips` (after the drain, the window
regains its equilibrium within eight evaluations), all in process with
the socket writer stubbed.

## 25. Compression becomes the sole clock exactly when the peer is recovering

From `handleSendItem`: the early acknowledgement fires on
`windowSize/2 <= sendSeq − ackedSendSeq`, where `windowSize` is the
window the NAT advertises (the ladder's rung, six megabytes at the
measured equilibrium, one megabyte at connection start unbudgeted) and
`sendSeq − ackedSendSeq` is what the peer has sent since the NAT last
acknowledged. The peer can have at most its congestion window
outstanding. So the signal fires only while the peer's congestion window
exceeds half the advertised window, and it cannot fire in slow start
after a timeout (one segment), at connection start (ten segments against
512 KiB), or for any flow whose window is small relative to the rung. In
those states the compression timer is the only acknowledgement clock,
and a peer whose window grows per acknowledgement received grows it once
per timeout. The measurement stream's 200 ms run, 246 samples at one per
198 ms and 136 KB per acknowledgement, is that regime made visible; the
structure is the same at 50 ms and only the ratio changes: a recovering
peer on a 50 ms path takes `(RTT + T)/RTT`, twice as long per growth
step, and on a 1 ms path fifty-one times as long. Confirmed as
structural, not a constant, and worst on the fastest paths, which is
where the previous program's lossy cells sit.

Whether the NAT can tell a recovering peer from an idle one without new
signalling: yes, from state it already holds. An idle peer has nothing
outstanding, `sendSeq == ackedSendSeq`. A recovering or slow-starting
peer sends its whole window as a burst and then stops, with bytes
outstanding and nothing arriving, because it is waiting for the
acknowledgement the NAT is withholding. The send loop sees every arrival
and the acknowledgement loop already owns `ackSignal`; "bytes
outstanding and no arrival for a short quiescence" is a third trigger
computable from those two facts, and it fires exactly once per burst,
which is the acknowledgement the peer needs and no more. Linux
receivers do the same thing in two forms this NAT lacks: an immediate
acknowledgement for the first segments of a connection (quickack) and
one per two full segments (RFC 1122), both of which keep a slow-starting
sender clocked. The shape of a remedy is therefore a third trigger on
arrival quiescence with bytes outstanding, plus a bounded quickack count
at connection start and after a gap, leaving the timer as the idle bound
it is now; not a smaller constant, which would leave the structure and
only move the ratio. Cost: one extra acknowledgement per burst, on a
path that is by definition sending less than half a window. No constant
is chosen here; the quiescence bound is the campaign's.

| Row | Test | Pins | Fails on | Regime |
|---|---|---|---|---|
| K1 | `TestHalfWindowSignalCannotFireBelowHalfTheAdvertisedWindow` | a peer sending bursts smaller than `windowSize/2` receives acknowledgements only at `AckCompressTimeout` intervals | none; characterises the structure | in-process |
| K2 | `TestAckIsNotWithheldFromABurstThatStopped` (after the remedy) | with bytes outstanding and no arrival for the quiescence bound, the acknowledgement leaves within that bound rather than at the timer | the tree as shipped | in-process |
| K3 | `TestRecoveryAfterATimeoutIsNotClockedByCompression` | a peer stack (the tun's gVisor) that takes one retransmission timeout on a 50 ms path recovers its window within a bound set by round trips, not by `AckCompressTimeout` multiples | the tree as shipped | in-process, tun and NAT |

### 25.1 The two claims, and the reading that replaces the inference

The measurement stream's evidence for the trigger is complete: five of
five collapsed runs show exactly one timeout retransmission and ten of
ten healthy runs none, the timeout fires because a held acknowledgement
crosses the peer's 200 ms floor rather than because anything was lost,
and one firing suffices because the smoothed estimate then adapts while
the collapsed window is starved. That is the first claim, and it needs
an absurd constant on a clean path.

The second claim is what §25 argues and is independent of the trigger:
any collapse of the peer's window, ordinary loss on a real path
included, is followed by acknowledgement-clocked growth that the
compression timer paces once the peer's window is under half of the
advertised one, which is every recovery. It reaches the shipping 50 ms,
it is worst on the fastest paths, and the state that would let the NAT
stop withholding is already local (bytes outstanding, arrivals stopped).
The remedy is the suspension §25 shapes, not a smaller constant, and it
is a design section and a campaign.

To observe the growth directly rather than infer it from rates, the tun
now returns a `TunTcpConn` that keeps its endpoint, and
`TunTcpConn.TcpInfo()` reads the stack's `TCPInfoOption`: `SndCwnd`,
`SndSsthresh`, `RTT`, `RTTVar`, `RTO`, `State` and `CcState`. Sampling it
through a collapsed run should show the congestion window stepping once
per compression interval after the timeout, which is row K3's reading.

## 26. Design: acknowledgements that follow the peer's recovery instead of the timer

Written before the confirming campaign, 2026-09-13. §25 established the
structure; this is the remedy developed to the point where the test
stream can build to it. Nothing here chooses a constant.

### 26.1 Severity first: worst on the fastest paths, and which direction it reaches

A peer whose window is below half of the NAT's advertised window is
acknowledged only by the compression timer, so each growth step of its
recovery takes `RTT + T` instead of `RTT`. At the shipping 50 ms the
penalty is a factor `(RTT + T)/RTT`: two at a 50 ms round trip, six at
10 ms, fifty-one at 1 ms. Lossy paths enter recovery more often; fast
paths pay far more per entry; and a same-datacenter rig, the reporter's,
is the regime where one loss costs the most. That is the headline of
this section.

Which direction it reaches must be stated with equal care, because §19
settled the direction from source and from measurement. The compression
paces the NAT's acknowledgements of bytes the client sends toward the
origin: it governs uploads, and the request half of any bidirectional
flow, and nothing about the origin-to-client stream. The reporter's
headline ceiling is a download, and this mechanism does not bear on it;
their uploads, which they did not measure, and the same-datacenter
regime are what it reaches. Saying otherwise would conflate the two
directions this program has just finished separating.

### 26.2 What the NAT already holds

Per flow, under the connection mutex or on the send loop: `sendSeq`, the
next byte expected from the client; `ackedSendSeq`, the last value
acknowledged, so `outstanding = sendSeq − ackedSendSeq` is what the
client has sent that we have not acknowledged; `windowSize`, the
advertised window; `initialSynSeq`, the client's initial sequence, so
`sendSeq − initialSynSeq − 1` is bytes received on the connection;
`peerMss`; the arrival of every segment through `handleSendItem`; and,
for every arrival, the disposition it already computes: in order (`start
== 0`), a retransmission of accepted bytes (`end <= 0`, `Stale`), or past
a hole (`0 < start`, `Retained` or `Rejected`). The last two already
produce an immediate duplicate acknowledgement (`sendCurrentAck`), so
the loss event itself is acknowledged at once today; what is not is the
in-order data that follows it. The acknowledgement goroutine already
takes `ackSignal`, a one-slot channel any rule may fill without blocking.

### 26.3 The predicate, the phase, and the single acknowledgement

Two rules, both from that state.

Recovery phase. Entered by the send loop when either holds:

- E1, loss evidence: an arrival with disposition `Stale`, `Retained` or
  `Rejected`. A peer that retransmits or sends past a hole has taken a
  loss event, and its window is collapsed or halved.
- E2, connection start: `sendSeq − initialSynSeq − 1 <
  StartQuickackByteCount`. A new connection's window is ten segments
  against an advertised half-window of hundreds of kilobytes.

While the phase is active, the send loop fills `ackSignal` on every
in-order arrival that brings the bytes since the last acknowledgement to
at least `QuickackEverySegments × peerMss`. That is the RFC 1122
receiver, applied only inside the phase. The phase exits on the first of:

- the bytes since the last acknowledgement reach `windowSize/2`, so the
  existing half-window rule is now the binding trigger and the peer is
  out of the small-window region;
- `RecoveryQuickackByteBound` bytes have been acknowledged since entry,
  which bounds what one event may cost;
- `outstanding == 0` for longer than `AckCompressTimeout`: the flow went
  quiet, and nothing is being clocked.

A new E1 after exit re-enters with fresh counters. A peer that never
leaves recovery, losing on every window, therefore re-enters on every
loss and pays the bound each time, which is the right outcome: a path
that loses on every window needs its acknowledgements, and the timer
never applied to it usefully.

Burst end. Independently of the phase, whenever `outstanding > 0` and no
arrival has occurred for `QuiescenceBound`, the send loop fills
`ackSignal` once. The peer sent what it may and stopped; it is waiting on
us. This catches the odd last segment of a burst that the every-k rule
leaves, and short flows whose whole request is under k segments. It is
armed only while `outstanding > 0`, so it costs nothing on an idle flow.

What separates a recovering peer from a quiet one is `outstanding`. A
quiet peer has acknowledged everything it sent and `outstanding == 0`:
no rule fires, the timer stays the only clock, and compression keeps
what it buys. A peer with bytes outstanding that has stopped sending is
either recovering or paused mid-stream, and in both cases one
acknowledgement after `QuiescenceBound` is what an ordinary receiver
would have sent within its delayed-ACK bound anyway.

### 26.4 What it costs, including when it fires wrongly

Worst-case acknowledgement rate while the phase is active: one per
`QuickackEverySegments` segments, at the peer's send rate; at 465 Mb/s
and two-segment spacing that is about 20,000 per second, which is what
a Linux receiver sends in the same state, and it lasts at most
`RecoveryQuickackByteBound` bytes per event. Outside the phase the rate
is the timer's `1/T` plus one per burst end, as now plus at most one.

Spurious entry. E1 on a reordered rather than lost segment enters the
phase for one bound's worth of acknowledgements, once. E2 costs one
bound per connection, which is what Linux's quickack at connection start
costs too. The burst-end rule on an application pause mid-stream costs
one acknowledgement per pause. None of these is ordinary idleness: with
nothing outstanding no rule fires. The compression's purpose, few
acknowledgements during continuous streaming with a large window, is
untouched, because in that state arrivals never stop and bytes between
acknowledgements reach the half-window before any quickack rule would.

Where the acknowledgements land matters for the cost: the NAT is on the
provider and its acknowledgements travel the tunnel to the client, so a
phone receives them. That is why the per-event bound exists and why the
campaign must measure it on a device, not only on the rig.

### 26.5 Parameters, and what sets each

| Parameter | Role | What the campaign measures to set it |
|---|---|---|
| `StartQuickackByteCount` | length of the connection-start phase | time to the first full window on a fresh upload at 1 ms and 50 ms, against acknowledgements received by the client, sweeping the count |
| `QuickackEverySegments` | acknowledgement spacing inside the phase | recovery time after a forced loss against acknowledgements per event, at 1, 2 and 4 |
| `RecoveryQuickackByteBound` | most one event may cost | acknowledgements per loss event on a device against the recovery time it buys, sweeping the bound |
| `QuiescenceBound` | how long a burst must be silent | burst-tail latency on a slow-starting peer against acknowledgements sent to a paused stream; the floor is timer granularity and the candidate scale is a fraction of the flow's measured inter-arrival time |

Prediction, stated for the campaign to test: post-loss recovery time at
1 ms round trip falls from tens of compression intervals to within a
small multiple of round trips; at 50 ms the gain is under a factor of
two; steady-state upload throughput at 50 ms is unchanged within the
null band; the 200 ms cliff's recovery is no longer starved, though the
spurious timeout that triggers it remains and stays bounded by row C1.

### 26.6 Tests, in the contract shape; the root cause is the starvation

| Row | Test | Pins | Fails on | Regime |
|---|---|---|---|---|
| Q1 | `TestSlowStartingPeerIsAckedPerBurstNotPerTimer` | a scripted peer in process (a `TcpSequence` with its writer stubbed) sends one segment, waits for the NAT's acknowledgement, sends two, four, eight, up to a window kept under half of a 1 MiB advertised window, with `AckCompressTimeout` at 200 ms and `QuiescenceBound` at 5 ms: every round's acknowledgement is emitted before the timer would have fired, so ten doublings complete in under a tenth of ten timer intervals | the tree as shipped, where each round waits the full timer and ten doublings take ten intervals | in-process; asserted on progress per round, with the margin a factor of ten so scheduling cannot flip it |
| Q2 | `TestRetransmissionEntersTheRecoveryPhaseAndTheBoundEndsIt` | after a `Stale` arrival, the next in-order segments are acknowledged every `QuickackEverySegments`; after `RecoveryQuickackByteBound` bytes, acknowledgements return to the timer and half-window rules | the tree as shipped | in-process |
| Q3 | `TestQuietPeerIsNotAckedEarly` | with `outstanding == 0`, no acknowledgement leaves for ten timer intervals; with `outstanding > 0` and no arrival, exactly one leaves after `QuiescenceBound` | a remedy that fires on idleness | in-process |
| Q4 | `TestConnectionStartQuickackIsBounded` | the first `StartQuickackByteCount` bytes are acknowledged every k segments and the bytes after them are not | the tree as shipped, and a remedy without the bound | in-process |
| Q5 | `TestRecoveryAfterATimeoutIsNotClockedByCompression` (K3) | the tun's gVisor peer takes one retransmission timeout on a 50 ms path and regains its window within a bound of round trips rather than of timer intervals; `TunTcpConn.TcpInfo` samples `SndCwnd` stepping per round trip | the tree as shipped | in-process, tun and NAT, the root cause end to end |

## 27. Design: a floor for every lane, and the grant order that the managers already use

Written before the discriminator, 2026-09-13, conditional on it. §20.5
showed that a nonzero lane keeps only one Pack in flight beside a lane
that holds the pool. Two designs, then a recommendation.

### 27.1 Option A: floors as exemptions, the pool unchanged

The floor is already a field the queue understands: `minByteCount`, the
part of a queue not borrowed from the pool. For a data lane it is set to
`LaneFloorByteCount` instead of zero, whether the lane borrows from the
shared lane pool or from a supplied device budget; the pool stays one
`ResendQueueMaxByteCount`. Under contention the heavy lane may still take
the whole pool, but every other lane keeps `LaneFloorByteCount` in
flight without asking the pool, which is what lane zero and every
distinct destination on an sdk-hosted provider have today.

What it costs. A floor is accounting, not allocation: bytes are consumed
only by queued packets, and an unopened lane's exemption is unused
headroom. A client whose flows hash to one data lane therefore pays
nothing for the seven it never opens, and the pool itself is created
lazily on the first nonzero lane as now. The worst case per client on
the bare provider is lane zero's own bound plus the pool plus the sum of
floors actually in use: `2 MiB + 2 MiB + 7 × LaneFloorByteCount`, which
at the 256 KiB the sequences already use across destinations is
5.75 MiB, of which the floors are 1.75 MiB and only while seven lanes
are simultaneously above zero. On an sdk-hosted provider the floors draw
on the device budget exactly as cross-destination floors do, and on a
phone they apply only to the lanes it sends on. Nothing is reserved.

What it does not give: fairness above the floors. Between two heavy
lanes the pool still goes to whichever runs first after a release.

### 27.2 Option B: the ordered grant the managers already use

`TransferMemoryBudget` has two ways to wait. Sequences use
`CapacityNotify`, a broadcast on every release, and then race `CanAdd`.
The WebRTC managers use `addCapacityWaiter` and
`notifyEligibleCapacityWaiters`, a FIFO grant that scans past a request
too large for the available capacity so a smaller one cannot starve,
and subtracts each grant from a capacity snapshot so one release does
not wake every waiter. Under B, a sequence whose `CanAdd` fails
registers a waiter for the next Pack's bytes and proceeds when granted;
a heavy lane re-registers after each grant behind the lanes already
waiting, so grants rotate among the lanes that want them, and a light
lane's share of the pool's drain is at least one grant per cycle
without any reserved floor.

What it costs. No memory. A change to the admission wait of every
sequence, including lane zero and the cross-destination sharing on
sdk-hosted providers, on a path every Pack takes; the waiter list is
touched only on a failed admission, so the per-Pack cost is a failed
`CanAdd` away, but the ordering semantics of a shared admission path
change for everything at once. It also fixes something A does not:
fairness among destinations above their floors on a device budget,
which today is the same broadcast race.

The divergence between the two waits looks accidental: the managers'
grant list exists because fixed-size lifetime owners needed an exact
ceiling, and the sequences predate it. That is an argument for B in the
long run, and it is not an argument for making the lane campaign carry
it.

### 27.3 Recommendation

A for the lane campaign, B as its own change afterward. A is one
assignment where the floor is zeroed today, reuses semantics proven
across destinations, costs one-lane clients nothing, bounds the
per-client memory in a sentence, and removes the starvation the
discriminator is about to look for, so the campaign's number generalises
past clean cells. B changes a hot admission path shared by every
sequence and deserves its own rows and its own measurement, on
destinations as much as on lanes; landing it inside the lane campaign
would make the lane count's number depend on two changes at once.
`LaneFloorByteCount` is the one parameter, and the campaign sets it by
the light flow's delivered rate beside a saturating lane against the
per-client memory at 4 and 8 lanes; the candidate scale is the existing
`ResendQueueMinByteCount`.

### 27.4 Tests, in the contract shape; the root cause is the starved light lane

| Row | Test | Pins | Fails on | Regime |
|---|---|---|---|---|
| F1 | `TestLightLaneKeepsItsFloorBesideASaturatingLane` | two data lanes on one pool of one cap; the heavy lane's queue is filled to the cap with nothing acknowledged; the light lane then admits at least `LaneFloorByteCount` bytes of Packs with no release from the heavy lane | the tree as built, where the light lane admits one item and blocks | in-process, synchronous admission, no timing |
| F2 | `TestOneLaneClientPaysNoFloor` | with eight lanes enabled and every flow hashing to one data lane, the pool's `UsedByteCount` never exceeds that lane's queued bytes and no pool exists before the first nonzero lane | a reservation implementation | in-process |
| F3 | `TestLaneFloorsAreExemptionsNotReservations` | with seven lanes idle and one active, the active lane borrows up to the full cap | a reservation implementation | in-process |
| F4 | `TestGrantOrderRotatesAmongWaitingLanes` (option B, if built) | with the pool full and three lanes waiting, releases are granted in waiting order and a re-registering heavy lane goes behind the others | the broadcast wait | in-process |
| F5 | `TestLightLaneDeliveryBesideASaturatingLane` (the campaign's in-process mirror) | with F1's shape and acknowledgements flowing, the light lane's delivered bytes per acknowledgement round trip stay above its floor and above one Pack | the tree as built | in-process |

### 26.7 Refinement: the counting rule is the remedy, the burst-end trigger is its tail

26.3 carried both rules but led with the wrong one. Worked through with
per-acknowledgement window growth, which is what the measured peer does:
a peer with W segments in its window sends W and stops. Acknowledged
once per burst, its window becomes W + 1: linear growth, and reaching
93 segments from one takes 93 round trips, 4.7 s at 50 ms, against the
timer's 18 s. Acknowledged every second segment, the window becomes
1.5 W per round; every segment, 2 W. The counting rule restores
exponential growth and the burst-end trigger alone would replace a
dependence on the timer with a dependence on burst count, one level
down. So the phase's every-`QuickackEverySegments` rule is the remedy,
and the burst-end acknowledgement is what catches a burst that ends
short of the spacing, so the round does not hang until the timer.

Why it is affordable, stated precisely, because the obvious condition
is wrong. "Bytes since the last acknowledgement are under half the
rung" is true at the start of every acknowledgement interval in steady
state, so a counting rule conditioned on it would fire every two
segments of a saturated upload, twenty thousand times a second. The
condition must be evidence that the peer's window is small, and that is
what the phase's entries are: E1, loss evidence, after which the window
is collapsed or halved; E2, connection start; and a third the campaign
should include, E3, resumption after an idle longer than
`AckCompressTimeout` with nothing outstanding, since a peer's stack
returns to slow start after idle without any loss. The phase exits when
a burst reaches `windowSize/2`, which is the peer's window having grown
out of the small regime, so the state in which the rule fires and the
state in which acknowledgements are expensive are disjoint by
construction: inside the phase the peer sends little, doubling from one
segment toward the half-window; outside it the half-window rule and the
timer are untouched. The bound per event is therefore the half-window
itself, about `(windowSize/2)/(QuickackEverySegments × peerMss)`
acknowledgements over the doublings, a thousand at a six megabyte rung
with two-segment spacing, and `RecoveryQuickackByteBound` exists only
for a peer that never grows, an application-limited sender that would
otherwise keep the rule alive.

A constraint derivable now: the burst-end wait plus the path's round
trip must stay well under the peer's retransmission floor, 200 ms in
gVisor and Linux, or the held acknowledgement fires the same spurious
timeout that produced the cliff; that bounds `QuiescenceBound` from
above before the campaign searches, and it is the same relationship row
C1 pins for the timer.

Row Q5 (K3) must assert the shape, not only a bound: recovery of a
window of N segments completes in a number of round trips logarithmic in
N, within a constant factor, so a linear recovery, which the burst-end
trigger alone would give, fails the row rather than passing as an
improvement. Row Q1 is restated the same way: ten doublings in a bound
of round trips, with the acknowledgement count per round rising with the
burst.

### 26.8 Implementer's note: the entry condition is evidence of a small window, never a byte count

Read this with 26.3 before writing the predicate. The counting rule of
26.3 and 26.7 must be gated on the recovery phase, and the phase is
entered only by E1 (a `Stale`, `Retained` or `Rejected` arrival), E2
(`sendSeq − initialSynSeq − 1 < StartQuickackByteCount`) or E3
(an arrival after `outstanding == 0` had held for longer than
`AckCompressTimeout`), and left when a burst since the last
acknowledgement reaches `windowSize/2`, when `RecoveryQuickackByteBound`
bytes have been acknowledged since entry, or when `outstanding == 0`
has held for `AckCompressTimeout`.

The simplification to avoid, stated as the invariant it breaks: outside
the phase, a saturated upload with a large window produces exactly one
acknowledgement per half-window or per timer interval, as it does
today. Any predicate of the form "bytes since the last acknowledgement
are under `windowSize/2`" or "outstanding is small" is true at the start
of every interval of every flow, would satisfy every recovery row in
26.6, and would acknowledge every k segments of every upload for ever:
twenty thousand acknowledgements a second at 465 Mb/s, on the client's
downlink. The recovery rows cannot catch that, so a row exists that
does.

| Row | Test | Pins | Fails on | Regime |
|---|---|---|---|---|
| Q6 | `TestSteadyStateUploadEmitsNoQuickacks` | a peer with a window above `windowSize/2` sending continuously for ten timer intervals with no loss, no start and no idle in the window: the acknowledgements emitted number at most one per half-window of bytes received plus one per timer interval, exactly the tree as shipped; the phase is never entered (`recovering` stays false) | an implementation whose entry condition is a byte count rather than E1, E2 or E3 | in-process; the row that guards steady state while Q1 to Q5 guard recovery |

The same invariant, read from the counters after any campaign run: on a
lossless steady-state upload the acknowledgement count on this tree
equals the count on main within the timer's jitter. If it does not, the
gate is wrong, whatever the recovery rows say.

### 26.9 Designer's answers: the quiescence bound's lower end, and arming without history

Two questions from the implementation stream, answered as design.

First, what the burst-end wait is for once the counting rule exists,
because that decides how much its bound matters. With acknowledgements
every `QuickackEverySegments`, a burst of W segments leaves at most
k − 1 unacknowledged at its end, and the peer is not stalled by them:
it has W − (k − 1) acknowledged segments, its window has grown, and the
next round's first counting acknowledgement covers the odd tail
cumulatively. The only stall is a burst smaller than k, which is the
first round after a timeout, when the window is one segment: no
counting acknowledgement can fire and the peer waits on us. That round
is the critical path of the whole recovery, and it should not wait on a
timer at all. So the phase gets a fourth rule, Linux's quickack: the
first `QuickackImmediateSegmentCount` in-order segments after entry are
acknowledged at once, one acknowledgement each, before the every-k rule
takes over. With that, the burst-end wait is a safety net for a burst
that ends short of k later in the phase, or a peer that stops with data
outstanding, and its latency is off the critical path.

That settles the lower bound. `QuiescenceBound` must exceed the gap
between segments of one burst as the tunnel delivers them, which is the
reliable carrier's Pack spacing rather than the peer's wire spacing,
because a burst crosses Transfer in Packs and arrives in clumps; below
that gap the trigger fires mid-burst and the cost is one extra
acknowledgement per false firing, a cost rather than a fault. Above the
derived upper bound of 26.7 it fires the spurious timeout. Because it is
now a safety net, the campaign should choose from the upper part of that
interval, not the lower: the measurement that sets it is the
distribution of intra-burst Pack inter-arrival on the reliable carrier
(the receive side already timestamps every Pack) against the
acknowledgement count on a paused stream, and the bound sits several
multiples above the distribution's tail.

Second, arming with no history. The trigger arms on the first arrival
after the phase is entered and re-arms on every subsequent arrival while
`outstanding > 0`, and disarms when an acknowledgement leaves that covers
everything outstanding. It needs no inter-arrival estimate, because it
asks only whether the last arrival was more than `QuiescenceBound` ago,
and a flow that has never been measured has nothing to estimate: the
bound is the same for every flow at entry. One timer per flow, reset per
arrival, only inside the phase, so the per-arrival cost is bounded by
the phase the way the acknowledgements are. The coordinator's reading is
right; it is the rule.

The parameter table of 26.5 gains `QuickackImmediateSegmentCount`,
set by acknowledgements per loss event against recovery time at one,
two and four, on a device.

| Row | Test | Pins | Fails on | Regime |
|---|---|---|---|---|
| Q7 | `TestFirstSegmentAfterATimeoutIsAckedAtOnce` | after a `Stale` arrival, a single in-order segment is acknowledged without waiting for the every-k spacing or the burst-end bound | a remedy with the counting rule alone, where the first round waits on the burst-end timer | in-process |
| Q8 | `TestBurstEndTriggerArmsOnFirstArrivalAndRearms` | in the phase, arrivals spaced under the bound produce no burst-end acknowledgement; the first gap over the bound with bytes outstanding produces exactly one; with nothing outstanding none | an arming rule that waits for an estimate | in-process |

### 13.6 The reporter's zombie figure is a bound saturated at the floor, and my prediction was at the ceiling

The implementation stream derives, from the shipped queue bound and the
minimum resend interval, that one dead destination can put at most
2 MiB per 2 s, 8.4 Mb/s, on the wire, and the report measured about 8.
§13.1 predicted 2.1 Mb/s, the same queue over the 8 s `MaxResendInterval`
that the backoff reaches after six rewrites. The two readings differ in
which interval a dead destination's items sit at, and the report's
number says the floor, not the ceiling. Either the reporter measured
inside the first twenty seconds after the kill, before the backoff had
climbed, or the backoff does not climb for a dead destination, which
would be a transfer-layer fact worth having on its own: `sendCount`
advances only on the `sendRecoveryNone` path, and a path that rewrites
without advancing it (a promoted head, a deferred item re-queued) would
hold the interval at its floor. Row Z1 decides it by reading the
interval directly, and the design records the disagreement rather than
adjusting either figure to the other.

What both readings agree on is the contribution to the reporter's open
question: forty zombies put at most 336 Mb/s on the wire at the floor
and 84 at the ceiling, against a measured loss of about 460. The
remainder, 124 to 376 Mb/s, is now a quantified gap rather than an
unexplained one, and it is what §13.3's other candidates and §13.4's
matrix are for.

### 26.10 Designer's answers: where the burst-end deadline lives, and what the counting rule counts

The measured rows first, because they change the weight of the choices:
53 acknowledgements at one per 11.3 ms against 12 at one per 50.2 on
the starvation row; the first segment after a timeout acknowledged at
once with the counting spacing wide and the burst-end trigger off, so
the fourth rule of 26.9 carries the round the counting rule cannot; no
mid-burst acknowledgement across six bounds of arrivals; and row Q6 at
21 acknowledgements against 29 allowed where the ungated counting rule
would give 52. The guard holds, and the recovery rows are passing for
the right reason.

Where the deadline lives. The compression wait computes its deadline
once, and a burst-end deadline moves with every arrival, so it cannot be
a shortened compression timeout; the implementer's silent first build
is the proof. Of the two mechanisms: (a) the send loop wakes the
acknowledgement goroutine on each in-order arrival inside the phase
through a second single-slot channel, and the wake recomputes the
deadline; (b) the acknowledgement goroutine arms its timer at the
deadline computed from a last-arrival timestamp the send loop stores
under the connection mutex, and a firing that finds the timestamp has
moved re-arms at the new deadline instead of acknowledging.

I intend (b). Its wakes are proportional to elapsed quiescence
intervals, not to segments: over a recovery from one segment to a six
megabyte half-window at 50 ms, (a) wakes about two thousand times and
(b) about a hundred, and at 1 ms (b) wakes a handful of times while (a)
still wakes per segment. Compression's purpose was fewer wakeups, and
the remedy should not reintroduce a wake per segment on the very flows
it is fixing, even bounded. The shared state is not new coupling: the
send loop and the acknowledgement goroutine already share `sendSeq` and
`ackedSendSeq` under `self.mutex`, the send loop already takes that
mutex per item, and a timestamp store there costs nothing. The
invariant (b) must keep: every wait start computes its deadline from
the shared state under the mutex, phase active and `outstanding > 0`
and `lastArrival + QuiescenceBound`, and a firing re-checks the same
state and either acknowledges or re-arms; the latency after the last
arrival is then the bound plus timer granularity, the same as (a). Phase
entry during a wait is covered without a dedicated wake, because the
immediate first-segment rule signals `ackSignal` on the next arrival and
the goroutine recomputes on re-entering its wait. (a) is acceptable if
the re-arm logic proves error-prone in review; rows Q3 and Q8 pin the
observable behaviour, not the mechanism, and pass either.

What the counting rule counts. Segments, not bytes: in-order arrivals
that carry payload, one count each, and an acknowledgement every
`QuickackEverySegments` of them. The quantity the rule clocks is the
peer's acknowledgement-counted growth, one step per acknowledgement
received (26.7, and the 200 ms fit), so the count that matters is
acknowledgements per segment received, and a byte rule against
`peerMss` under-acknowledges a peer whose segments are small, which is
exactly what the row with segments below the fallback showed. RFC 1122's
"full-sized" qualifier exists to keep a receiver from acknowledging
runs of tiny segments; here the phase's own exits already bound that
cost, since a small-segment flow is application-limited and leaves the
phase by the quiet exit, and the immediate-first-segments rule already
acknowledges tiny segments at entry. So `peerMss` is not an input to
the rule, and the harness's missing segment-size option is then
irrelevant to it; keeping the explicit value in the fixture is fine for
whatever else reads it. `RecoveryQuickackByteBound` stays in bytes
because it also relates to the half-window exit, with the note that a
bound in acknowledgements would express the cost more directly and the
campaign may prefer it.

| Row | Test | Pins | Fails on | Regime |
|---|---|---|---|---|
| Q9 | `TestCountingRuleCountsSegmentsNotBytes` | in the phase, segments of one quarter of `peerMss` are acknowledged every `QuickackEverySegments` segments, the same spacing as full segments | a byte-based rule | in-process |
| Q10 | `TestBurstEndWakesPerIntervalNotPerArrival` (mechanism (b)) | across a burst of N arrivals spaced under the bound, the acknowledgement goroutine wakes at most once per bound plus one, not N times | mechanism (a), by construction; a characterisation if (a) is chosen | in-process, wake counted through a test hook |

## 28. Design review of the §26 and §27 implementations

Read against §26 through §26.10 and §27: `d19b861` (the counting rule),
`0cd8f05` (the immediate first segments and the burst-end trigger),
`ac3f117` (the lane floor), and the rows in `ip_tcp_ack_starvation_test.go`
and `transfer_lane_floor_test.go`. Both §26 commits predate §26.10, so
its two rulings are listed first as known; the rest was found by reading.

### 28.1 What matches

The entry predicate is the three evidence conditions and nothing else:
E1 at both loss dispositions (a gap at `0 < start`, a retransmission at
`end <= 0`), E2 on `sendSeq − initialSynSeq − 1 < StartQuickackByteCount`,
E3 on an arrival with nothing outstanding after a quiet longer than
`AckCompressTimeout`, measured from `quiescentNanos`, which the
acknowledgement goroutine stamps whenever it covers everything. No byte
count enters the phase, the half-window branch clears it, and the byte
bound is applied where the acknowledged bytes are counted. The shipping
default of zero for `QuickackEverySegments` disables the phase so trees
stay comparable. The four state fields live under `self.mutex` beside
`ackedSendSeq`, as specified. The immediate-first-segments rule consumes
its counter per in-order arrival and signals `ackSignal`, coalescing
under the one-slot channel as intended. The burst-end trigger disarms
when an acknowledgement covers everything (`lastArrivalNanos = 0`). The
measured rows agree with the design's predictions: 107 segments drawing
53 acknowledgements is two per acknowledgement as configured; the first
segment after a timeout acknowledged with the spacing wide and the
trigger off is the fourth rule carrying its round; and the steady-state
guard at 21 against 29 allowed, where an ungated rule gives 52, is the
disjointness of §26.7 holding in the code. The lane floor is the one
assignment §27.1 named, applied for every nonzero lane whether it borrows
from the lane pool or a device budget, as an exemption rather than a
reservation, with the pool unchanged.

### 28.2 Deviations no row would catch

1. Entry is not idempotent, and E2 re-enters on every arrival. `enterRecoveryWithLock`
   sets `recovering`, zeroes `recoveryAckedByteCount` and refills
   `recoveryImmediateSegmentCount` every time it is called, and E2 calls
   it on every in-order payload arrival while the connection is under
   `StartQuickackByteCount`. Two consequences the design did not intend:
   during the start window every segment is acknowledged at once, not the
   first N and then every k, because the immediate counter is refilled
   before each arrival consumes it; and the byte bound can never end the
   start phase, because its counter is zeroed on each arrival. E1 has the
   same shape on a run of stale arrivals, a go-back-N retransmission after
   a spurious timeout: every stale segment refills the immediate counter
   and zeroes the bound. The design intends: a fresh entry, from not
   recovering, sets all three; E2 and E3 while already recovering do
   nothing; E1 while recovering refills only the immediate counter, since
   a new loss is a new collapse, and leaves the bound's counter alone so
   a peer that keeps losing pays the bound and re-enters afresh, as §26.3
   says. The row that catches it is Q4 as specified, asserted exactly:
   over the start window, N immediate acknowledgements plus one per k
   segments of the rest, and not one per segment; the current file has
   no start-window row and no byte-bound row (Q2), which is why this
   passed.
2. An overdue burst end at wait start waits the full compression timeout.
   `quiescenceRemaining` is computed at the top of the wait and applied
   only when positive; when the acknowledgement goroutine returns from a
   slow emission (the pure ACK's `receivePacket` is a synchronous
   admission and can block) after a burst has already been silent for the
   bound, the remaining is negative, the cut is skipped, and the burst-end
   acknowledgement waits `AckCompressTimeout`. That is the starvation
   reappearing on exactly the slow acknowledgement path. The design
   intends: with the trigger armed and bytes outstanding, a non-positive
   remaining means acknowledge now. Row: `TestOverdueBurstEndIsAckedAtOnce`,
   in process, a burst ending while the acknowledgement path is held by a
   test hook, the acknowledgement leaving on release without a timer
   wait.
3. The burst-end mechanism is the hybrid of (a) and (b): a second wake
   channel per arrival and a timestamp with a re-check on firing. §26.10
   intends (b), and the code already has all of (b): a firing that finds
   the deadline moved falls through `continue` and recomputes from
   `lastArrivalNanos`. Deleting `arrivalSignal` and its per-arrival send
   yields (b) exactly, with the wake count proportional to elapsed bounds
   rather than to segments; row Q10 pins it.
4. The counting rule counts bytes against `peerMss` (with an MTU
   fallback). §26.10 rules segments: a per-phase counter of in-order
   payload arrivals since the last acknowledgement, compared against
   `QuickackEverySegments`, with `peerMss` not an input. Row Q9.
5. The third exit, quiet for `AckCompressTimeout`, is not present as an
   exit; its observable effect is supplied by E3's fresh entry on the
   next arrival after such a quiet, which resets the counters. That is
   equivalent for behaviour and I accept it, provided the comment says
   so, because `recovering` reads true through a quiet period and anyone
   exporting the phase state would otherwise be misled.
6. E3 fires on a connection's first data whenever it arrives more than
   `AckCompressTimeout` after the sequence started, since `quiescentNanos`
   is stamped at start; so the start phase is entered by E3 as well as by
   E2 for most connections. Harmless, because connection start is a
   small-window state either way, but it means `StartQuickackByteCount`
   is not the only gate on start behaviour and the campaign that sets it
   should know that.

### 28.3 The lane floor, and one deployment rule

`ac3f117` matches §27.1 and the test takes the starved lane from one
write to four with the floor set and back to one with it zero. The
default of zero preserves every earlier tree, as intended, with one
consequence to record: with the default, enabling lanes ships the
starvation §20.5 found. Lanes and the floor are one decision, and the
campaign that sets `LogicalDataLaneCount` sets `LaneFloorByteCount` in
the same change; a nonzero lane count with a zero floor is not a
configuration this program endorses. Row F3 (floors are exemptions) and
F2 (a one-lane client pays nothing) are still owed; F1 is what the
landed test is.

### 28.4 Verdict

The implementation is the design in structure and in the measured rows,
and it is not yet the design in four places that no row would catch:
the non-idempotent entry (28.2.1), the overdue burst end (28.2.2), the
hybrid wake (28.2.3) and the byte-based count (28.2.4). Items 1 and 2
change behaviour a campaign would measure, and item 1 would flatter the
start-window number by acknowledging every segment; both should land
before the §26 cells run. Items 3 and 4 are §26.10's rulings and are
already with the implementer. Rows to add: Q2, Q4 exact, Q9, Q10, and
`TestOverdueBurstEndIsAckedAtOnce`.

### 28.5 Two resolutions: rows F2 and F3 re-read, and what the overdue burst end was

Rows F2 and F3 landed in `ac3f117`, which §28.3 wrongly recorded as
owed; corrected. Re-read against what they were to establish:

F3, `TestLaneFloorsAreExemptionsNotReservations`, asserts the intended
property. One active lane beside seven idle ones puts nearly the whole
pool on the wire (60,416 of 65,536); a reservation implementation would
leave it `total − 7 × floor`, 8,192 with these numbers, so the row
discriminates by a wide margin whatever floor the campaign picks.

F2, `TestOneLaneClientPaysNoFloor`, asserts two things: no pool exists
before a nonzero lane sends, which is the lazy creation §27.1 relies on
and is exactly right; and, after one lane of eight sends, that the pool
is not full (`UsedByteCount < TotalByteCount`). That second clause
discriminates only by the numbers chosen: with eight lanes at an 8 KiB
floor against a 64 KiB pool, a reservation would fill it exactly and
fail, but at a 4 KiB floor a reservation would read 32 KiB used and the
row would pass while asserting the wrong property. The property intended
is that the seven unopened lanes are charged nothing, which is
`UsedByteCount == the active lane's queued bytes − LaneFloorByteCount`,
or at least `UsedByteCount ≤ the active lane's queued bytes`, and it
holds independent of the numbers. That is the one change to make before
the lane cells run; the 9,840 the row logs is consistent with it and the
row should assert it rather than log it.

The overdue burst end (28.2.2): a defect that would have existed had the
acknowledgement goroutine been able to be away for the bound, and it
cannot be on the production path. Its emission is
`receivePacket(packet, receiveRecoveryModeRegenerableControl)`, which the
provider admits through the non-blocking return shard, dropping on a
full queue rather than waiting, and which the cell's tun writes
synchronously in microseconds; so at every wait start the last arrival
is at most microseconds old and the remaining is positive. My review
said the pure ACK's admission "can block"; it cannot, by the same
callback rule this document cites elsewhere, and the premise was wrong.
What was true was narrower: the arithmetic treated a non-positive
remaining as no cut, which is a latent defect reachable only under a
hook that holds the emission, and the explicit check is correct hygiene
at no cost. The implementer's finding that no threshold separates the
two versions is the expected result of that, not a weakness of the row:
they differ only when the emission is held past the bound and the burst
produced no `ackSignal` of its own (immediate count exhausted, fewer than
k segments), a shape that needs a hook, not a timer. The arming row pins
the reachable property, and restoring the deviation failing it is the
right evidence. The measured start-window and steady-state movements,
seven against five expected where the deviation gave twenty-five, and
sixteen against twenty-nine where an ungated rule gives forty-seven, are
the two behavioural fixes doing what §28.2 said they would.

## 29. Three closures: the zombie remainder re-weighted, the compression floor ruled, and why the pool contract is hard to satisfy in a fixture

### 29.1 The zombie interval, and what the remainder now asks of §13's candidates

The implementer read a dead destination's resend interval directly: two
doublings, then pinned at the 8 s ceiling inside twelve seconds. So
`sendCount` advances for a destination that never acknowledges, there is
no timer defect, §13.1's 2.1 Mb/s per zombie is the steady state, and the
reporter's 8 Mb/s describes the first seconds after a kill. Forty
zombies put about 84 Mb/s on the wire in steady state against a measured
loss of about 460, so bandwidth accounts for under a fifth of the
coupling and the remainder, about 376 Mb/s, is what the other candidates
must carry, nearly the whole effect rather than a minority of it.

That changes their weight. A candidate whose cost per zombie is its
bytes cannot carry it: the transport write path and the exchange's
forwarding, both linear in zombie bytes, are demoted. A candidate whose
cost per zombie is what it pins is promoted, and one fits the threshold
shape: the message pools. Each zombie's resend queue holds up to
`ResendQueueMaxByteCount` of pool buffers for as long as it lives, 80 MiB
at forty, independent of its resend rate; once the pinned buffers exceed
the pool's free capacity every `MessagePoolGet` on the live flows'
packet path falls through to a fresh heap allocation, which does not
return to the pool, so the live traffic runs at its full packet rate as
a garbage-collected allocation rate with a large pinned live heap behind
it. The cost per zombie is then super-linear at the point the pool
exhausts and roughly flat before it, which is 8 at −11, 16 at −27 and 40
at −72 in shape. The instrument is `MessagePoolStats()` for the
fall-through rate beside `runtime.MemStats` (`NumGC`, `PauseTotalNs`)
and live throughput, swept over the zombie count as §13.4 specifies;
the prediction is that fall-throughs begin at the zombie count where
pinned resend bytes cross the pool's free capacity and that live
throughput falls with the fall-through rate from there. Row:
`TestPinnedResendQueuesTurnTheLivePathIntoHeapAllocation`, in process,
N never-acknowledged sequences holding their bound while one live
sequence's packet path is sampled for pool misses, asserting the miss
rate rises from zero at the pool's capacity boundary. The sdk-hosted
shared budget stays the other promoted candidate on that host class.
If the sweep shows no fall-through at forty, both are dead and the
remainder is in the transport's per-pack service time after all, which
the same sweep's CPU profile would then have to show.

### 29.2 The ruling: the compression floor is a test assertion, and why not the alternatives

§22.3 named the shape and this makes it the decision. The measurement
stream has shown the relationship is an identity: moving the peer's
retransmission floor to 400 ms moved the cliff to exactly 400, with the
collapse depth scaling as the mechanism predicts. A runtime guard cannot
hold it, because the floor that matters is the peer's, and the peer is
the client's stack, gVisor under our tun on some platforms and the
kernel's TCP on others (Linux and Darwin at 200 ms, Windows at 300), a
value no provider can read at runtime; a guard would assert against a
copy of a number it cannot verify. A comment cannot fail. A test
assertion can, and it can read the one instance of the constant we do
ship: `tcp.MinRTO` is exported from the vendored stack at 200 ms, and
`TunSettings.TcpMinRto` is ours when a campaign sets it.

So row C1 is the ruling, stated exactly:
`TestAckCompressionStaysUnderTheRetransmissionFloor` asserts
`DefaultTcpBufferSettings().AckCompressTimeout ≤ tcp.MinRTO / 4`, and
`≤ DefaultTunSettings().TcpMinRto / 4` whenever that is positive, and it
names the cliff in its failure message with the 400-for-400 evidence. The
quarter is the margin the shipping value has today, made explicit; a
campaign that lowers the timeout only widens it, a campaign that raises
either floor must move the ratio in the same commit, and a gVisor update
that moves `MinRTO` fails this row on the day it lands rather than in a
provider's upload months later. The fleet floor for kernels we do not
ship is recorded beside it as the constant `minimumPeerRetransmissionFloor`
at 200 ms with its provenance, so the assertion is against the lowest
floor a peer can have rather than against our tun alone.

### 29.3 Why the pool contract is hard to satisfy when writing a fixture

Two new cells, one session, two ownership violations. A violation is a
buffer returned or shared that no owner held, an over-return; a leak is
the opposite and only the boundary reconciliation
(`MessagePoolOutstandingByteCount`) sees it. So both fixtures returned a
buffer something else had already taken, and the reason that is easy to
do is an asymmetry the entry points do not name.

The three rules of CODESTYLE are right. What they do not say is which
rule a given entry point applies, and the same packet is treated
differently one layer apart. A sequence-level entry takes the buffer:
`TcpSequence.receivePacket`, `UdpSequence.receivePacket` and
`receiveBatch` emit to the receive callback and return the buffer
afterwards themselves, so a fixture that calls them must not return it.
A callback-level entry borrows it: `LocalUserNat.receiveTransfer*`,
`RemoteUserNatProvider.Receive*`, `ReceiveBatch` and
`receiveTransferWithRecovery` hold it for the call, share what they keep
(`MessagePoolShareReadOnly` is a second reference on the same buffer,
and the original stays the caller's), and return nothing, so a fixture
that calls them must return it after the call. A send entry takes it on
success only, so a fixture must read the result before deciding who
returns. `DataPackets` allocates new buffers the caller owns. A fixture
author who has just watched a callee visibly keep a share, or who
imitates the sequence's own return-after-callback at the wrong layer, or
who returns "to be safe" after a send that succeeded, produces exactly
the violation the handler catches; the batch callback's `true` return
compounds it, since it reads as a transfer and means only "delivered".

What would make the correct pattern obvious, as a design rather than a
fix: every function that receives a pool buffer states one of three
words in its doc comment, borrows, takes, or takes on success, and
CODESTYLE's pool section lists the entry points under those three
headings so the rule is looked up rather than inferred; fixtures invoke
borrowing entry points through one helper that returns the buffer after
the call (`withBorrowedPacket(packet, func())`), which makes the
under-return impossible to forget and the over-return impossible to
write; and every fixture's cleanup runs the boundary reconciliation, so
the leak direction is caught as reliably as the over-return now is. One
live example to check under that reconciliation before it is trusted:
`startUnreachableProviderReturn` in the reporter's tests calls the
provider's borrowing entry with a copied packet and never returns it,
which is the under-return the handler cannot see.

### 27.5 Retained bytes, the provider-side asymmetry, and the one line that makes it inert today

The retention factor goes into the cost, explicitly. §27.1's figures are
queue accounting, and the measurement stream found the process holding
about 1.94 times the accounted bytes in pooled roots at one, four and
eight lanes, because a queued item retains its whole pool class: about
2.1 KB of content in a 4 KiB root at the measured payload. The ratio is
payload-dependent, near two at that fit and lower where payloads fill
their class, so it is a factor to measure per payload profile rather
than a constant; and one small term joins the derivation, since a queue
with no budget headroom still admits one item, so every sequence may sit
one item above its allowance, three to four kilobytes at the measured
payload and sixteen to twenty-nine at 8 KiB. A memory ceiling is sized
against retained bytes, so §27.1's worst case reads: accounted
`2 MiB + 2 MiB + 7 × floor`, 5.75 MiB at a 256 KiB floor; retained, about
1.94 times that plus the one-item term, near 11 MiB per client on a bare
provider.

Where that lands decides the rollout. On a provider it is affordable and
on the side with room. On a phone with one destination, a sending lane
count would cost its upload side, at the 32 MiB budget, lane zero's
1 MiB plus a 1 MiB pool plus the floors, roughly 3.75 MiB accounted and
7 MiB retained, most of a ceiling this program deferred rather than
dismissed, for an upload path that has one client's flows and does not
need lanes. So the asymmetry is the right rollout: lanes are a
provider-side setting, the phone keeps `LogicalDataLaneCount` at zero
and pays only its receive side, where the data lanes share one pool of
`ReceiveQueueMaxByteCount` (1.25 MiB at 32 MiB, about 2.4 MiB retained)
holding only out-of-order items, plus fixed per-sequence state. That
receive-side figure is the device cell's to confirm.

The negotiation supports it. Receivers stamp `transferLogicalLaneVersion`
on their acknowledgements independently of their own sending count, a
sender creates a nonzero-lane sequence only after the destination's
lane-zero class has acknowledged support, and an old client that never
does keeps every sender at lane zero. So a provider may hash its returns
onto lanes toward any client that advertises, with no client-side setting.

Except that it does not, as the code stands, and this is the finding.
`selectLogicalLane` gives an explicit `TransferKey` precedence: a Pack
sent with a key reproduces that key's lane, and only a Pack without one
hashes by the sender's count. The provider's return path sends every
return with `providerReplyTransferKey`, which copies the client's whole
key, `LogicalLane` included, and flips only `CompanionContract`. A client
with its count at zero sends on lane zero, so its key says lane zero, and
every provider return to it is pinned to lane zero by that key whatever
the provider's own count is. A provider-only lane count is inert on the
one direction lanes were built for. The comment on `LogicalLane` says
lanes "are reproduced on replies", which is the routing-keys rule for
protocol replies, and the provider's IP returns are not replies in that
sense: they are the provider's own ordinary traffic whose ordering domain
is the sender's choice, negotiated only by the receiver's capability.
Nothing at the client keys anything on the return's lane; each lane is an
independent receive sequence delivered in its own order, and a flow's
outbound segments on lane zero and inbound on a hashed lane have no
ordering relation TCP requires. So the change is one field: the reply
key reproduces the session identity, `ForceStream`, `EncryptionRole`,
`EncryptionCompanion` and the contract policy, and not the lane, and the
resolution treats a reply key without a lane as not explicit so the
provider hashes by its count under the existing capability gate, using
the same flow key H3 flow isolation already computes
(`ipSendSchedulingKey`). Provider-side only; no client changes; old
clients unaffected by the gate.

Rollout, then: enable the count and the floor together on providers
only, with that change landed first, and the campaign's number is a
provider setting. Enabling lanes on clients to get provider returns onto
lanes would be paying the phone's upload memory for the provider's
benefit, and it is ruled out.

| Row | Test | Pins | Fails on | Regime |
|---|---|---|---|---|
| L5 | `TestProviderReturnsHashToItsOwnLanesRegardlessOfTheClientsLane` | a client sending on lane zero with its count at zero, a provider with count eight and a floor: the client's receive side sees the returns of distinct flows on distinct lanes one to eight by flow hash | the tree as it stands, where every return arrives on lane zero | in-process |
| L6 | `TestProviderReturnsFallBackToLaneZeroForAClientWithoutLaneSupport` | the same provider toward a client whose acknowledgements carry no lane version: every return on lane zero, no nonzero sequence created | a change that hashes past the gate | in-process |
| L7 | `TestClientWithLanesOffPaysOnlyTheReceivePool` | a client at count zero receiving on eight lanes: its send buffer holds one sequence and no lane pool; its receive buffer holds one shared lane pool at `ReceiveQueueMaxByteCount` and its retained roots read within the measured factor of that | a client that opens send lanes to receive | in-process, with the pool reconciliation |
| L8 | `TestLaneCostIsReadInRetainedBytes` | the §27.1 accounting plus the one-item term against `MessagePoolOutstandingByteCount`, reporting the ratio; asserted only that retained is at least accounted, since the ratio is the payload's | none; the instrument for the device cell | in-process |

## 30. The discriminator adjudicated: the fixture's client binds first, and no verdict about the relay survives it

Data: lane zero, one flow 276.6 Mb/s, eight flows 509.1, ratio 1.84;
count eight, one flow 241.3, eight flows 420.5; per-flow spread at eight
flows 2.67 and 3.84. The entry condition (eight flows about equal to one
at lane zero) failed, and every one of 202,000 packs on the count-eight
arm rode lane zero.

### 30.1 The reading the evidence supports

The second. Two numbers decide it before any interpretation of what
Transfer or a relay does. The reporter's signature is one flow reaching
about 665 Mb/s and eight flows reaching the same; this fixture's one
flow reaches 277 and its eight reach 509, so the fixture cannot enter
the regime in which the reporter's serialization shows, because
something else caps it at less than half the reporter's single-flow
number. And 1.84 is the ratio the provider-upstream cell produced with
no Transfer layer at all: adding the entire layer moved the flow scaling
by nothing, which is what a bound downstream of both cells produces, and
this program has already located that bound in the harness's gVisor
client, flat across a sixty-four-fold provider window and a thirty-two-
fold client window (§19.2). A fixture bounded below the regime cannot
eliminate anything in the regime. So the first reading's promotion of
the relay by elimination is not supported: Transfer without a relay did
not reproduce the signature in a harness that could not have reproduced
it with or without one. The relay keeps exactly the prior §20 gave it,
as one hop of the serialized per-client chain, neither promoted nor
demoted.

### 30.2 Why the lane arm never engaged, from source, and what it says about the fixture

Hashing onto a data lane needs two gates, not one (`selectLogicalLane`):
the lane-zero class of the destination must have advertised support, and
the Pack must carry a valid scheduling key
(`sendPack.schedulingKey.valid`); a Pack without one returns lane zero
before the version is consulted. The provider's return path sets that
key from the flow (`ipSendSchedulingKey`, the same key H3 flow isolation
uses) on every return; a fixture that sends through any other entry, a
`Send*` without the scheduling key option, never hashes, whatever the
acknowledgements say. The advertisement itself is unconditional on the
receiver: every acknowledgement it writes carries
`transferLogicalLaneVersion`, on both the v2 frame and the protobuf
path, the parse keeps it, and the sender records it in
`observeLogicalLaneVersion` only from the live lane-zero sequence and
only after the acknowledgement matched an item in its resend queue. So
"the acknowledgement apparently never completes" has two possible
causes and both are readable in process: `logicalLaneVersions[base]` for
the destination's base key, and the scheduling key's validity on the
Packs the fixture sends. If the key is invalid the fixture's send path
is not the provider's return path, which is the concrete way it differs
from a real client and a real provider, and it is repaired by driving
the fixture through `ReceiveBatch` and the return path rather than
through a bare send. That is the prerequisite for any lane verdict, and
the harness stream's investigation should read those two values first.

### 30.3 A cost the run did measure without meaning to

With the count at eight and no lane ever engaged, the arm ran 13 to 17
per cent below the lane-zero arm. Nothing in the packet path differs
between those arms except the gate itself, and the gate takes the send
buffer's mutex on every Pack to read `logicalLaneVersions[base]`, a lock
shared by every sequence of the client, while a count of zero returns
before the lock. That is a per-Pack acquisition of a buffer-wide lock
added by enabling a count, and it is the likely cost. It is a finding
independent of the fixture's fidelity and it must not be shipped inside
a lane rollout: the version belongs on the sequence, pushed by the
buffer when it changes, so the per-Pack gate is a lock-free read. Row:
`TestLaneCountGateDoesNotTakeTheBufferLockPerPack`, asserting under a
lock-contention hook that the gate acquires no buffer mutex on the Pack
path.

### 30.4 What would separate the two readings, and what the run leaves standing

A cell whose client can reach the reporter's single-flow number: a
kernel-stack client behind a real tun in a network namespace, or the
reporter's own client, so that one flow reads about 665 and the question
"do eight flows read the same" can be asked at all; then lane zero
against lanes, and no relay against a relay hop, each as its own arm.
Until then neither in-process cell measures the reporter's ceiling, and
the remedy question stays where §20 and §27 left it: designed,
implemented, pinned in process for its own properties (F1, the floor
rows), and unmeasured for throughput. The entry-condition rule did its
job, the harness stream's refusal to soften it was right, and the run's
two real yields are the second gate and the per-Pack lock.

## 31. Where the factor goes: the cost of one byte on the download path, and three structural candidates sized honestly

A decomposition from source and from what this program has measured, not
a campaign. The gaps to explain: 34 Gb/s kernel-to-kernel on one host
against about 300 Mb/s in the provider-upstream cell, a factor of 113;
and the reporter's 665 Mb/s on one flow against 2,680 for WireGuard on
the same hosts, a factor of four, with eight flows buying nothing.

### 31.1 The stages, per byte and per packet

The path of a downloaded byte from the origin socket to the application
inside the client, with what each stage does to it. "Pass" means the
byte is read or written by the CPU once; "handoff" means a goroutine
hands work to another goroutine through a channel or a lock and the
receiver must be scheduled.

| Stage | What happens to the byte | Per byte | Per packet or Pack | Inherent or implementation |
|---|---|---|---|---|
| origin kernel to `socket.Read` | copy to the 64 KiB read buffer | 1 pass | one syscall per 64 KiB | inherent to a userspace proxy |
| `DataPackets` | split into MTU segments: pool buffer per segment, payload copy, IP and TCP headers, checksum over the payload | 2 passes | 1 pool get, 1 header build, per 1,448 B | implementation: a userspace NAT re-originating TCP toward the client |
| reader to batch consumer (`readPackets`) | none | 0 | 1 handoff per packet, amortised per batch of up to 64 | implementation |
| provider callback: policy inspection, share, item, frame | header parse, refcount, frame build | 0 | per packet: parse, share, `ipPacketFromProviderFrame`; per Pack of up to 16 packets or 24 KiB: one item, one admission | implementation |
| `SendBuffer.Pack` to the sequence goroutine | none | 0 | buffer mutex, sequence lookup, admission, 1 handoff per Pack | implementation; per client serialised from here |
| sequence goroutine: marshal, session cipher, resend queue | Pack serialised (copy), AEAD over the Pack (crypto pass and copy into ciphertext), ciphertext retained for resend | 3 passes, 1 of them crypto | per Pack: contract accounting, queue insert, 1 handoff to the transport writer | marshal copy is implementation; one crypto pass per hop endpoint is inherent |
| transport writer (H1) | websocket framing, masking if the client role masks, TLS record AEAD, copy into the record, kernel write | 3 to 4 passes, 1 crypto | per record | the second crypto layer is implementation: the session cipher already encrypts, TLS to the exchange is transport hygiene |
| exchange ingress resident | kernel read, TLS decrypt, unmask, frame parse, shard handoff | 3 to 4 passes, 1 crypto | 1 to 2 handoffs per frame | the relay is a structural choice; every relayed byte pays two extra TLS terminations and a hop |
| exchange forward to the destination resident | internal connection write and read | 2 passes | 1 handoff per frame each side | relay |
| exchange egress to the client | TLS encrypt, framing, kernel write | 2 to 3 passes, 1 crypto | per record | relay |
| client transport reader | kernel read, TLS decrypt, unmask, Pack parse | 3 passes, 1 crypto | 1 handoff per Pack | second crypto layer again |
| client receive sequence | session decrypt (crypto pass and copy), ordering, frame delivery | 2 passes, 1 crypto | per Pack: queue, ack generation (a Transfer ack per Pack back up the whole chain) | inherent crypto; the ordering domain is implementation (§20) |
| client device write | copy into the tun; on a device app this is the kernel's tun and the kernel's TCP; in the harness and headless hosts it is gVisor's inject and gVisor's TCP | 1 to 2 passes | per packet: tun write; in gVisor, per segment: checksum, reassembly, endpoint lock, and an inner ACK generated every second segment | tun copy inherent; gVisor is implementation on hosts that use it |
| the inner ACK path back | every second data segment produces a 60-byte ACK that rides the whole chain in reverse: tun read, Pack, session AEAD, TLS, exchange, TLS, provider, session decrypt, NAT `applySendAckWithLock` | none per data byte | one full chain traversal per 2 segments, about 28,000 per second at 665 Mb/s | inherent to end-to-end TCP through a tunnel; its per-packet cost is implementation |
| application read | copy out of the client stack | 1 pass | per read | inherent |

Counting: about 25 passes per delivered byte, six of them crypto (two
session, four TLS at the four transport endpoints), against WireGuard's
roughly five passes and one crypto pass per hop with no relay. And
about ten to fifteen goroutine handoffs per 1,448-byte packet across the
chain at the per-packet stages, plus the ACK chain.

### 31.2 Where the factor goes

The passes do not explain the numbers. Twenty-five passes at a memory
or crypto pass rate of one to four gigabytes per second per core bound
a pipelined chain near one to two gigabits per core, above both measured
figures. What matches them is the per-packet work on a serialised chain.
At 665 Mb/s a 1,448-byte packet arrives every 17 µs, and a chain that
has ten to fifteen handoffs per packet, at one to two microseconds of
scheduling each, plus the per-packet header, checksum, pool, policy and
tun work, spends about that long per packet on its critical path. So the
reporter's single-flow number is the chain's per-packet service time,
and the way to read the 4x is: WireGuard handles a packet in the kernel
in about a microsecond with one crypto pass and no handoffs, at 64 KB
super-segments where the host offloads; we handle it in about 17 µs
across four processes and two encryptions, at 1,448 bytes. Eight flows
buy nothing because the chain is one sequence, one transport writer,
one exchange path and one client reader per client, so more flows queue
behind the same per-packet service (§20). The 113x in the cell is that
plus two things the cell adds: the harness's gVisor client, whose TCP
receive path costs about four times its UDP path per segment (1.39 Gb/s
UDP against 0.3 TCP through the same tun and NAT), and the comparison
against a loopback stream whose segments are 65 KB, so that most of the
113 is the ratio of segment sizes rather than of per-packet efficiency.

Two conclusions the decomposition forces. The dominant lever is the
number of packets per byte, not the number of passes; and the second
lever is the number of serial stages per client, not any window or
timer. Everything this program has swept, windows, compression, buffer
pins, lanes' gate, is a percentage on a chain whose shape is the cost.

### 31.3 Candidate one: the per-client ordered sequence

Real, and the reporter's eight-flow result is its signature. Honest
size of the prize: it is an aggregate prize, not a single-flow one. One
flow at 665 is the chain's service time and no amount of concurrency
changes it; eight flows could approach eight times only if every
per-client serial stage were parallelised, and the sequence goroutine
is one of four (sequence, transport writer, exchange path, client reader
and receive sequence). Lanes parallelise the sequence at both ends and
nothing else, so by Amdahl their aggregate prize is bounded near 1.5x
until the transport is also parallel, which means several exchange
connections per client, which the multi-route writer's shape allows and
nothing today configures. Lanes as designed are the right idea for the
part they cover and were done badly in two places this program found,
inert on downloads because the reply key pins the lane (§27.5) and a
buffer lock per Pack in the gate (§30.3); fixed, they are worth their
share and not more. Ordering per flow rather than per destination is
the right semantic and lanes approximate it by hashing; true per-flow
sequences would be thousands per client and, because contracts and
sessions are per sequence today, would multiply contract requests by the
flow count, so it needs contracts decoupled from ordering domains first.
Verdict: worth pursuing for aggregate throughput, in the order transport
parallelism then lanes then per-flow domains; a dead end for the
single-flow gap, and it should not be sold as one.

### 31.4 Candidate two: the client's network stack

The framing needs a correction before the assessment. Device apps do not
terminate TCP in a userspace stack: on iOS, Android, macOS, Windows and
Linux the app injects packets into the OS tun and the kernel's TCP owns
the connection, exactly as wireguard-go does on the same platforms. The
gVisor stack under `tun.go` is the client on headless hosts, proxies,
the harness, and whatever the reporter's rig used as its client, which
must be established because it decides whether their 4x contains this
term at all. Where gVisor is the client, the cell says its TCP receive
costs about four times its UDP receive per segment, and the path that
avoids it is a kernel tun on those Linux hosts, letting the kernel own
TCP and the client forward packets as the apps do: a multiple, up to
the UDP-to-TCP ratio, for exactly those hosts, and zero for the apps.
For every client, the lever that remains is the one WireGuard uses to
be fast in userspace: fewer, larger packets through the tun boundary.
Our inner MTU is 1,280 to 1,500 and it never touches a physical link
inside the tunnel; only the carrier does, and Transfer already chunks
Packs to the carrier. A 16 to 64 KB inner MTU divides every per-packet
stage in 31.1, including the inner ACK chain, by ten to forty-five,
leaving the per-byte passes as the bound at one to two gigabits per
core. What would have to be true: the client OS's tun accepts the MTU
(Linux and macOS utun do; iOS and Android limits need checking, and
they may cap it), the inner TCP negotiates its MSS from it (it does, via
the SYN through the tunnel), the provider emits large segments (one
`DataPackets` change), the pool has a large class, Transfer carries a
frame larger than an H3 datagram chunk (it does not today: it splits
groups at frame boundaries, so datagram carriers need frame
fragmentation or a stream carrier), and the loss cost per lost frame is
acceptable on reliable carriers, which it is. Verdict: the inner MTU is
the one candidate on this list that is a multiple on every client and
on the single-flow number, and it is a change of moderate size with
platform preconditions that must be verified before it is promised.

### 31.5 Candidate three: the reliability layer

The reporter's claim tested: "the provider terminates TCP and keeps no
copy, so Transfer is the only retransmitter" is true of the terminating
user NAT and is not inherent to a provider. Someone must hold a copy of
unacknowledged bytes; the question is who. Today Transfer holds it in
the resend queue. The NAT could hold it instead, as a real TCP sender
toward the client with its own retransmission driven by the client's
duplicate acknowledgements and SACK, and Transfer could carry data
frames unreliably with reliability kept for control. Honest prize: not
throughput. The per-byte passes are not in the reliability layer, and
the per-Pack bookkeeping it adds is a few per cent; what it would buy is
loss recovery at the inner TCP's cadence (one round trip, fast
retransmit) instead of Transfer's timers with their 300 ms floor and
8 s ceiling, and the removal of cross-flow head-of-line blocking (§20.2),
which is a latency and robustness prize on lossy paths and nothing on a
clean one. Honest size: a TCP sender's loss recovery inside the NAT,
which is large, for a gain the throughput cells cannot see. Verdict: a
dead end for the 4x on user-space providers, and it should stop being
listed as a throughput candidate.

The version of it that is not a dead end is the one WireGuard actually
uses: keep the origin as the retransmitter by not terminating at all.
A provider with kernel privileges can be a kernel NAT rather than a
userspace one: inner packets go to a tun, the kernel masquerades to the
origin, return packets come back on the tun and go down the tunnel.
The provider then keeps no TCP state, `DataPackets` and the per-flow
socket readers disappear, the origin holds the retransmission copy, the
inner TCP is end to end, and Transfer's reliability is unnecessary for
data because the endpoints have their own. That removes the provider's
per-packet NAT work and two of its passes, and it removes the
head-of-line and timer costs for free. Its size is a second provider
mode for server-class hosts only, since phones cannot do it, and it
does nothing about the exchange or the client. Verdict: a real
candidate for server providers, worth perhaps a third of the chain's
per-packet cost plus the loss-path gains, not a multiple by itself, and
the natural companion of the inner-MTU change on such hosts.

### 31.6 What fell out that was not on the list

Two structural costs the decomposition surfaced that none of the three
candidates names. The inner ACK chain: every second data segment sends
a 60-byte ACK through the whole chain in reverse, about 28,000 per
second at 665 Mb/s, each paying every per-packet stage; at high rates
that is a large share of the chain's CPU, and it scales with the inner
packet count, so the inner MTU removes it and nothing else does short of
thinning ACKs at the client's tun, which is §22's tradeoff on the other
side and carries the same recovery caveats. And the double encryption:
the session cipher and TLS both encrypt every byte at every transport
endpoint, six crypto passes where one per hop is inherent; dropping TLS
where the session cipher is in place is a percentage, ten to twenty on
CPU, and it is listed so it is not mistaken for a multiple. The relay
itself is the third: a direct route removes two TLS terminations, a hop
and its handoffs, and the previous programs found direct routes fail to
connect on Android, which is where the relay is most expensive.

### 31.7 Summary for the choice

| Candidate | Prize on the single-flow gap | Prize on aggregate | Size of the change | Must be true |
|---|---|---|---|---|
| inner MTU of 16 to 64 KB | a multiple, bounded by the per-byte passes at one to two gigabits per core | the same | moderate: tun MTU, `DataPackets`, a pool class, Transfer frame fragmentation for datagram carriers | client OS tun limits; carrier framing; loss cost per frame |
| kernel NAT provider mode | about a third of the chain's per-packet cost, plus loss-path gains | the same, plus no head-of-line | large, server hosts only | privileges; a second provider mode |
| kernel tun for gVisor-hosted clients | up to about four times on those hosts, zero on apps | the same | moderate, Linux hosts only | which client the reporter measured |
| transport parallelism then lanes | none | up to a few times, bounded by the client's chain | moderate, with §27.5 and §30.3 first | ordering rows L2 and L3 |
| dropping Transfer reliability on user-space providers | none | none on clean paths | large | a dead end for throughput |
| dropping the second crypto layer | ten to twenty per cent | the same | moderate | a percentage, listed so it is not sold as more |

The first row is where a multiple lives on every client and on the
number the reporter measured; the second and third are multiples for
particular hosts; the fourth is aggregate only; the fifth is a dead end.

### 31.8 The direction asymmetry: what the download path carries that the upload path does not

The pin pair's number that matters here: in the same harness, same
process, same tun and same client stack, an upload reaches 665 Mb/s
(pinned, 12 ms compression) while a download tops near 300. So the
harness's client is not uniformly expensive per byte, and 31.4's simple
form, "the userspace stack is the cost", is too coarse. The two paths
differ in three stages, all per packet, and all three are on the
download side.

Upload, from source: the client stack's send path segments the app's
write, emits segments to the tun, the harness reads them, the NAT's
dispatch shard parses and hands each to its flow's send loop, and the
socket writer gathers up to 64 payloads into one vectored write that the
kernel copies once. The reverse traffic is the NAT's acknowledgements,
which the NAT compresses to one per timer interval or per half window:
tens per second, each processed by the client stack's send side as one
cumulative acknowledgement that frees hundreds of segments at once.

Download, from source: the NAT reads 64 KiB and `DataPackets` segments
it, one pool buffer, one payload copy, one header and one checksum pass
per 1,448 bytes; each segment crosses a channel to the batch consumer,
which injects it into the tun; the client stack's receive path verifies
the checksum, takes the endpoint lock, queues or reassembles, delivers,
and generates an acknowledgement every second segment; and every one of
those acknowledgements comes back through the tun, the harness reader,
the NAT's single dispatch shard, the flow's `sendItems` channel and
`handleSendItem`, where `applySendAckWithLock` advances the window and
wakes the reader. At 665 Mb/s that would be 28,000 reverse packets a
second, each paying every per-packet ingress stage.

So the stages absent from the upload path are: segmentation at the NAT
(the kernel does it for the upload, on a vectored write); the client
stack's receive path per segment (the cell measured it at about four
times its UDP receive; the send path's cost is bounded above by the
upload result itself); and the inner acknowledgement chain at TCP's
native rate, which the upload never pays because our NAT compresses its
own acknowledgements and nothing compresses the client's. That last one
is the sharpest reading of the asymmetry: downloads pay for
uncompressed inner acknowledgements through the whole chain, uploads do
not, and the difference is about a 28,000-packet-per-second reverse
stream at the reporter's rate. On the reporter's path those packets do
not stop at a dispatch shard: each becomes its own Pack unless the
client batches its tun reads, with its own marshal, session cipher, TLS
record, exchange forward and provider receive, at roughly eight times
the Pack rate of the data itself.

What this does to candidate two: the specific version stands and is
narrower than 31.4. For gVisor-hosted clients the receive path per
segment is a real term and the kernel tun removes it; for every client
the two terms on our side of the tun remain, `DataPackets` and the
acknowledgement chain, and both are packets-per-byte costs that the
inner MTU divides and nothing else on the list touches. The cheap lever
the asymmetry names by itself is acknowledgement thinning at the
client's tun, the mirror of the NAT's compression, which carries §22's
and §25's caveats in the other direction and is not designed here. The
namespace cell's acceptance test follows: reach the reporter's figure on
a download, and record packets per second in each direction beside the
throughput, because the reverse stream is the number that decides which
term binds.

### 31.9 Two candidates from the implementation stream, weighed against the ordering domain

The lane reading is confirmed by direct reading: a real provider with a
real return path and a count of eight returns on lane three only when
the client is on lane three, and on lane zero when the client is; the
explicit reply key short-circuits before the count, the version and the
scheduling key are consulted, the scheduling key is valid on every
return, and the version is never reached. §27.5's one-field change is
what the row asserts.

The send buffer's client-wide mutex, taken once per Pack for the
sequence lookup. What it protects: the sequence maps (`sendSequences`,
the wire and sequence-id indexes, the destination index), against
creation and retirement racing with lookup. Whether it is needed on the
Pack path: a lookup needs only a consistent snapshot, so a copy-on-write
map read atomically, with creation re-checking under the lock, is
correct and removes the acquisition; the implementation stream did the
same for the gate. Whether it is the flow-scaling limit: no, and I must
correct §30.3 in the same breath. Packs are groups of up to sixteen
packets, so the Pack rate at 665 Mb/s is about 3,600 per second and in
the cell about 1,600; an uncontended mutex acquisition is tens of
nanoseconds, a contended one perhaps a microsecond, and at those rates
either is under a thousandth of a core. The 13 to 17 per cent I
attributed to the gate's second acquisition cannot be that lock, and the
cell in which the flow scaling was measured has no Transfer layer and no
send buffer at all, so the lock cannot be in its 1.84 either. The
unconditional acquisition is of the same order as the gate's, which is
to say negligible, and replacing it is hygiene. The ordering domain is
not this lock wearing its clothes: the domain's cost is one goroutine
per client doing every Pack's marshal and cipher in series, and the
fixes are different because the costs are different. The 13 to 17 per
cent stays unexplained until the arms are rerun with the gate's
acquisition removed, which the implementation stream has already done;
if the gap remains, it is elsewhere in the arm, and if it vanishes I was
right for the wrong reason and will say so.

The local NAT's `SendShardCount`, defaulting to one. Why it is one, from
the settings' own comment: each shard's dispatch channel holds
`SequenceBufferSize` slots that pin in-flight pool buffers under
backpressure, so shards multiply pinned memory, and the count sits
beside the memory-scaled buffer size for that reason; flows pin to a
shard by address tuple so per-flow order survives any count. What the
shard does per packet: parse the headers, look up the flow under the NAT
mutex, hand off to the flow's channel, one to three microseconds. At the
rates in question, 57,000 upload packets a second at 665 Mb/s or 28,000
download acknowledgements, that is six to seventeen per cent of one
core, real and not binding; it binds at several gigabits, which is where
this program wants to be and is not. So it is a genuine per-client
serialization, cheap to enable, costed in pinned memory by design, and
not the current bound; the row that would show when it binds is the
dispatch shard's occupancy and CPU share beside throughput, and enabling
it should wait for that reading rather than for optimism.

Neither displaces the ordering domain in the aggregate story, and
neither touches the single-flow number, which 31.2 and 31.8 place in
the per-packet stages and the acknowledgement chain. The merge of the
runtime pin rule stands as a regression fix: eighteen per cent of upload
at the shipping compression and twenty-six at 12 ms on this host is what
an unconditional deletion would cost, twelve of twelve paired.

## 32. The Transfer window as a bandwidth-delay ceiling: which cap binds, what divides it, and the two levers

The user's hypothesis, derived from source rather than from arithmetic.
It is the hypothesis §18 listed as its second item and §20.2 left live
for the reporter's path after it was ruled out, correctly, for the cell
that has no Transfer layer.

### 32.1 Which cap binds, and on which side

A send sequence has two caps in two units, and they gate two different
populations. `SequenceBufferSize` (`defaultTransferBufferSize`, 32 items)
sizes the `packs` channel and the pack admission: Packs a caller has
handed to the sequence that its goroutine has not yet taken, sent and
enqueued. `ResendQueueMaxByteCount` (2 MiB unscaled, per lane) bounds
the resend queue: Packs that have been written to the carrier and not
yet acknowledged. The loop moves a Pack from the first population to the
second only while `resendQueue.CanAdd` says the byte bound has room, and
it stops draining the channel when it does not, so the channel fills to
32 and callers block in admission. The in-flight window that divides
into the round trip is the second population, in bytes; the first is a
burst buffer in front of it, in items, and it does not enter the
bandwidth-delay arithmetic. It bounds only how far a caller may run
ahead of the goroutine, 32 Packs of up to 24 KiB, and a caller that
outruns it blocks on the goroutine's service rate, not on the path. So
on the send side the byte cap binds, at every latency, and the sweep
should scale with it proportionally until something else does.

Two other caps sit near it. The carrier's own flow control: on H3 the
stream receive window autotunes from 256 KiB to
`MemoryScaledByteCount(3 MiB, 384 KiB)`, above the 2 MiB queue unscaled
and above the 1 MiB scaled queue at the 32 MiB budget, so the Transfer
queue binds first on both, but a byte-cap sweep past 3 MiB on H3 will
stop scaling at the carrier's window, and the sweep must say which
carrier it ran on. And the transport's kernel send buffer, autotuned to
`tcp_wmem[2]`, 4 MiB stock, above the queue; §15's rule does not apply
to transport sockets and need not.

Amended after the sweep (§36). The constant-queue sweep that tested this
section's arithmetic ran on an in-process carrier with no H3 window and
no carrier socket, so neither cap above was on its path; its plateau is
an inner-path window (§36.4), and this section's naming of the H3 window
as the next ceiling was wrong for that cell. The H3 ceiling stands as a
prediction for a real carrier, where it binds at 3 MiB (§36.2), and the
memory scale can lower it but never raise it (`memory_budget.go:79–84`).

The receive side. `ReceiveQueueMaxByteCount` (2.5 MiB unscaled) bounds
the out-of-order queue: Packs held above a hole until the hole fills; on
an in-order path it is empty and never binds, and under loss it caps how
much of the sender's window survives at the receiver while the hole is
recovered. `SequenceBufferSize` on the receive side, 256 items, sizes the
handoff channel from the transport reader to the receive sequence: a
burst buffer again, in items, and it binds only when the sequence
goroutine falls behind the reader, at which point the pump refuses and
counts a drop. The asymmetry between 32 and 256 is deliberate and about
the reliable-carrier handoff rule, not about windows: a reliable lane's
reader may not drop what it has read, so its handoff is deep; a caller
into a send sequence may block, so its handoff is shallow. Neither is a
window. There is no receiver-advertised window in Transfer at all, which
matters below.

### 32.2 What divides the window: the effective acknowledgement round trip, decomposed

The quantity is the time from a Pack's write at the provider to its
acknowledgement being applied at the provider's resend queue. From
source, in order:

1. Carrier and network, provider to exchange to client: the path's
   one-way delay plus queueing in the transport socket buffers. Inherent
   as network; the socket queueing is bufferbloat of our own if the
   window exceeds what the path carries, and it is bounded by the window
   itself, window over rate.
2. The client's receive: transport reader, session decrypt, ordering,
   delivery, then the acknowledgement. Deliver-before-ack, so an ack
   waits on delivery to the device. Microseconds to a millisecond of
   processing, and one term we chose: `ReceiveBufferSettings.AckCompressTimeout`,
   10 ms, which holds the cumulative acknowledgement so that every
   received message does not emit its own ack frame. Average five
   milliseconds added to every acknowledgement's round trip, ten at
   worst, and it is the largest term we own on this path.
3. The acknowledgement's return, client to exchange to provider: the
   network again, plus the client's transport writer, where the ack
   frame queues behind whatever the client is sending, which on a
   download is its inner TCP acknowledgements at TCP's native rate
   (§31.8), thousands of small frames a second; a queueing term of our
   making, bounded by that writer's service rate.
4. The provider applies it: `ackMessageDetailed` to the ack worker to
   the window, and the loop wakes on `ackNotify` at once; no timer of
   ours on this side. Scheduling only.

Two corrections to the refinement. The NAT's 50 ms `AckCompressTimeout`
is the inner TCP acknowledgement of client uploads (§19.1) and is not in
this divisor on a download; the term that is, is the receive buffer's
10 ms. And the tunnel settings' comment about an effective round trip
orders of magnitude above loopback, fixed by raising a 256 KiB window,
describes the inner TCP layer under gVisor, whose acknowledgements cross
the whole tunnel twice; it is the same shape one layer down and the
precedent is real, but its 300 ms is not this layer's number.

Whether the divisor is mostly inherent or mostly ours is not decidable
from source, because it depends on the rig, and the reporter's two
statements point opposite ways: three hosts in one datacenter subnet
would make the network sub-millisecond, in which case an effective
25 ms is ours and mostly the 10 ms compression plus queueing; a distant
provider hosted to get a baseline would make the network the divisor,
in which case the queue is the only lever. The quantity is already
measured and only needs reading: the sequence's `RttWindow` samples the
Transfer acknowledgement round trip per Pack for the resend timer, and
its mean beside a ping between the hosts says which case the rig is.
That reading is the first thing the measurement stream should take,
before the sweep, because it decides which lever is worth anything.

### 32.3 The ceiling, derived

For a download to one client on the reporter's path: throughput is
bounded by `ResendQueueMaxByteCount` over the effective acknowledgement
round trip of 32.2, 2 MiB over the round trip, on every flow to that
client together, since they share the sequence. At 25 ms that is 671
Mb/s; the reporter measures 665. Eight flows buy nothing because the
window is per destination. A short path does not show it because the
window covers a short path's bandwidth-delay product with room to
spare, which is why a clean environment looks fine and a distant
provider does not. WireGuard has no reliable per-peer window and no
such ceiling. Beneath it sits a second, stacked window: the inner TCP's
receive window at the client over the inner acknowledgement round trip,
which crosses the tunnel twice and includes the full Transfer path both
ways; with a kernel client at 6 MiB it sits above, with a gVisor client
at 4 MiB just above, and on a phone whose `tcp_rmem` maximum is one or
two mebibytes it binds first. Both windows must be read before either
is raised.

### 32.4 The two levers, with their costs, unpicked

Raise the window: throughput rises in proportion until the next cap
(the carrier's window on H3, the inner window, then the per-packet
service rate of §31.2), and memory rises in proportion, in retained
bytes near twice the accounted ones (§27.5), on the side that holds the
sequence. On a download that is the provider, unbudgeted, but not
unbounded: a provider with forty concurrent bulk downloaders at 16 MiB
each holds 640 MiB accounted and over a gigabyte retained, so the
window cannot be a per-sequence constant; it has to be a share of a
provider budget, which is the floors-and-borrowing machinery §27
already reasons about. A phone's own uploads use its own window and
need little, since a phone's upload bandwidth-delay product at 50 Mb/s
and 50 ms is about 300 KB and its scaled window is already 1 MiB at the
32 MiB budget; so this is a provider-side change in the same sense
lanes are. The phone does pay on the receive side, in a way that is
not memory for throughput but memory for loss: a larger provider window
means more Packs arrive above any hole, and the phone's out-of-order
queue (1.25 MiB at 32 MiB) drops what it cannot hold, which the sender
recovers by retransmission at a cost in bandwidth and in the recovery
shapes the previous program measured (§34 there). Transfer has no
receiver-advertised window to make the sender respect that queue, and
adding one is a protocol change; without it the phone's queue is a
loss-cost term, not a cap.

Shrink the divisor: the same throughput for no memory, by removing what
we added to the round trip. The receiver's 10 ms acknowledgement
compression is the named term; at 665 Mb/s and 24 KiB Packs an
uncompressed receiver would send about 3,600 ack frames a second
instead of about a hundred, each a small frame through the session
cipher and TLS on the client's uplink, which is nothing on a desktop
and a wakeup question on a phone, the same question §22 asked of the
other layer and with the same shape of answer (a rate-dependent
trigger already exists there in the half-window signal; here the
equivalent would be an ack per N Packs or per fraction of the sender's
window, so that a large window is acknowledged often enough to keep
its bandwidth-delay product and a small one is not chattier than
today). The client's transport-writer queueing behind inner
acknowledgements is the other term of ours, and it is the same reverse
stream §31.8 named, which the inner MTU thins. The network term is
inherent. If the rig's reading in 32.2 says the effective round trip is
near the ping, this lever is empty and the queue is the whole decision;
if it says 25 ms on a sub-millisecond ping, this lever is worth what the
queue is worth and costs nothing.

### 32.5 The shape of the window fix, if the queue is the lever

Size the window from the path with memory as the ceiling rather than
the sole term, in the form receivers already use for their own windows:
`window = clamp(k × delivered over the last acknowledgement round trip,
floor, ceiling)`, with `k` at least two so that a window-limited flow,
which by definition delivers exactly its window per round trip, doubles
each round trip until it is no longer window-limited, and then holds.
The instrument exists: `deliveredBytesOver(ScaledRtt)` and the
`deliveredBytes` ring, built for `ReliableAdmissionBoundedByDelivery`,
which applied it in the other direction, to bound admission below the
queue, and shipped off because it cost transfer time; here it raises the
bound above today's constant and can only add. The ring is retained
only when its flag is on, so turning this on costs its 256 bytes per
sequence.

The estimate's quality: it needs the delivered count and the
acknowledgement round trip, both already measured; the round trip
enters only as the measurement interval, so an estimate that is too
short shrinks the delivered count and the window (the safe direction),
one too long grows it (the memory direction, bounded by the ceiling).
Wrong high: memory up to the ceiling, and up to one round trip of extra
queueing for the client's other flows, since a window above the
bandwidth-delay product sits in queues; that is the head-of-line cost
of §20.2 made larger, and the reason `k` should not exceed two. Wrong
low: today's behaviour. At connection start the window is today's
constant and doubles per round trip while window-limited, reaching a
16 MiB ceiling from 2 MiB in three round trips, under a tenth of a
second at 25 ms, so the converging phase is never worse than now.

The floor is today's bound, the ceiling is the sequence's share of a
provider budget with floors and borrowing rather than a per-sequence
constant, the parameters are `k`, the ceiling, and the budget's floors,
and a campaign sets them by throughput against retained bytes per
client at 25, 50 and 100 ms. On the receive side the out-of-order
queue's relation to the sender's window is the open question a
receiver-advertised window would close; until then the receive queue
is sized for loss recovery, and the campaign should measure drops at
the client under one induced loss at each window.

### 32.6 If the derivation and the sweep disagree

The sweep raises the byte cap from half a mebibyte to eight at fixed
latency and predicts proportional scaling. The derivation predicts the
same on H1 up to the inner window and the per-packet service rate, and
on H3 a stop at 3 MiB unscaled where the carrier's stream window takes
over. If the sweep stops scaling below 3 MiB on H1, one of us has the
wrong population: either the item cap gates more than the pre-send
buffer, which the loop's drain says it does not, or the inner window is
smaller than assumed, which the client's advertised window would show.
If it scales past 3 MiB on H3, the carrier's window is not what
`H3MaxStreamReceiveWindowByteCount` says or the run was not on H3. Say
which rather than reconcile.

| Row | Test | Pins | Fails on | Regime |
|---|---|---|---|---|
| W1 | `TestSendWindowIsTheResendQueueNotTheItemCap` | a sequence with a 4-item channel and a 2 MiB queue over an in-process link with 20 ms of acknowledgement delay reaches 2 MiB in flight; with a 32-item channel and a 256 KiB queue it reaches 256 KiB | a reading in which the item cap is the window | in-process |
| W2 | `TestThroughputScalesWithTheResendQueueAtFixedDelay` | at 20 ms acknowledgement delay, throughput at 4 MiB is about twice that at 2 MiB and at 8 MiB about four times, within the null band | a tree with another cap under 8 MiB | in-process, H1-shaped link |
| W3 | `TestReceiverAckCompressionIsInTheRoundTrip` | the sequence's `RttWindow` mean rises by the receiver's `AckCompressTimeout` when it is raised from 0 to 10 ms on a zero-delay link | none; characterises the divisor | in-process |
| W4 | `TestDeliverySizedWindowConvergesInLogRoundTrips` (after the fix) | from a 2 MiB floor at 25 ms delay the window reaches its 16 MiB ceiling within four round trips and holds at twice the delivered rate when the link is slower than the ceiling | the tree as shipped | in-process |

## 33. A diagnostic surface: decisions are made on snapshots, and the snapshot is the surface

A view on the class rather than on the fifth accessor. Four
investigations in this program stalled on the same shape: the quantity
that decides a mechanism question (the lane version a sender has
learned, the peer's congestion window, a Pack's scheduling-key validity,
a sequence's acknowledgement round trip) was unexported and unreachable,
while every volume, counts, bytes, outstanding totals, was public. The
public surface describes what happened; nothing describes what a
decision saw.

### 33.1 The rule

Separate the two surfaces by a criterion rather than by taste. A
quantity that a predicate in the code reads when it decides something
belongs on a mechanism surface; a quantity that only accumulates belongs
on the operational one. A round-trip mean is read by the resend timer
(§26's timing, §32's divisor), so it is mechanism state; that it is also
a useful operational summary does not change where it lives, and it can
be summarised onto the operational struct as well. The lane version is
read by `selectLogicalLane`; the scheduling key's validity is read by
the same predicate; the outstanding count and the last acknowledgement
are read by `abandonSilentSource`; the recovery phase and its counters
are read by the acknowledgement rules; the window rung and its blocking
counters are read by the ladder. Every one of them is an input to a
decision, and the principled surface is: the decision's inputs.

The way to make that hold by construction, rather than by remembering
to add a reader: a mechanism decides on an explicit snapshot struct,
built once at the decision point from the state it reads, and the
diagnostic surface returns that same struct. `selectLogicalLane` would
build `laneDecision{count, version, keyValid, legacy, lane}` and both
choose on it and expose it; the abandon rule would build
`silenceDecision{outstanding, sinceLastAck, carrierAbsent, silence,
bound}`; the resend timer's inputs are the `RttWindow` estimate. Adding
an input to a predicate then adds it to the surface, because they are
the same value, and a reader cannot lag a decision.

### 33.2 Unsampled is a fact, not a zero

Every estimate on the surface carries its own evidence: the value, the
sample count, and the age of the newest sample, so a zero with zero
samples reads as unsampled rather than as fast. This program was misled
by that ambiguity twice, in the deviation timer that could not cover an
unsampled stall (FLIGHTGATEFIX §36.10) and in the receive-side round-trip
precondition of §16.2, and the harness stream names it exactly. The
type should make the mistake unwritable: an estimate is a small struct
or a value-and-measured pair, never a bare duration, and a consumer that
wants "the round trip" must say what it wants when there is none.

### 33.3 Where it lives, and for whom

Not a build tag: the audience includes a support engineer with a
customer's slow connection, who needs it in the shipped binary. Not a
test seam: seams change control flow and exist in process; the harness
runs real binaries and cannot reach a seam, which is precisely why it
kept needing readers. A named, read-only, unstable surface in the
package, per component and keyed the way support asks: per source at
the provider (`RemoteUserNatProvider` already keeps
`providerSourceDiagnostics` and publishes them to the source through
`publishProviderDiagnostics`, which is the delivery path to the client
app a support engineer would read), per destination and per sequence at
the client (`DestinationSendStats` is the interim home, and the
round-trip estimate the implementation stream is adding there is right
to add now), and per flow at the NAT for the ladder, the recovery phase
and the client's advertised window. Reading allocates only when read,
snapshots under the lock the decision already holds, and promises
nothing about stability from release to release; the operational
structs keep their promises and their counters. The peer's congestion
window is the one exception, because it is the other stack's state and
lives behind the tun accessor of §25.1 rather than on ours.

### 33.4 The cost, and what not to do

Each reader so far has been a pure read with no behaviour change, and
the snapshot rule keeps it that way; what it adds is a small struct per
decision point that already had those values in locals. What not to do
is the fourth framing: telling a measurement stream to read through
test seams. It cannot, and the accretion of one-off readers is the
symptom of that answer having been given implicitly. The view recorded:
approve the round-trip mean now as an interim reader, and make the next
mechanism change, the recovery phase or the delivery-sized window, the
first to decide on a snapshot struct that is also its surface, so the
shape is established where the next question will be asked.

## 34. Eighty-five milliseconds on a twenty-millisecond line: what the estimator measures, what could inflate it, and the one reading that decides

### 34.1 The estimator, from `transfer_rtt.go`: staleness is not the explanation

A sample is `receiveTime − time.UnixMilli(tag.SendTime)`. The sender
stamps `SendTime` from its own clock in milliseconds when it builds the
Pack's frame (`OpenTag` inside the frame build, both the v2 and the
legacy path), the receiver echoes that tag on the acknowledgement, and
the sender closes it with its own clock on receipt (`CloseSendTime`), so
there is no cross-clock skew and at most one millisecond of truncation.
The receiver's coalesced cumulative acknowledgement carries the tag of
the newest item that advanced the head (`sequenceAckWindow.Update`
replaces the head ack, tag included, on a higher sequence number), so a
compressed acknowledgement measures its newest item's round trip plus
the compression it waited, not the oldest item's. `Estimate()` is the
plain arithmetic mean of the last 128 samples younger than 60 s, with no
decay and no weighting; `NewestSampleAge` says only how long ago the
last acknowledgement arrived. A mean of ten samples reading 85 to 99 ms
is ten acknowledgements that each measured 85 to 99 ms on average. So
the cheapest explanation is dead: the mean measures round trips, not
how rarely it is updated, and the reading's staleness is the reading
having been taken after the transfer ended.

Where the stamp sits relative to the wire matters and is right: the
Pack is taken from the scheduler only while `resendCapacity` holds, the
frame is built and tagged then, and the write follows in the same
iteration. No capacity wait and no contract wait sits between the stamp
and the write. What can sit there is the write itself blocking, when
the multi-route writer's transport cannot accept the bytes; in the
fixture that is the synthetic wire's data half, on a real path the
kernel send buffer, and in both it is counted into the sample.

### 34.2 What is between a byte's write and its acknowledgement's application

Named, with what each is, on the sender's own clock:

1. The blocking part of the write, if any: the transport refusing the
   bytes until it drains. A choice of ours only in that the window may
   exceed what the transport holds; zero in a fixture with an unbounded
   data half.
2. The data half of the path: inherent; in the fixture, nothing.
3. The receiver's transport reader, session decrypt, ordering, and
   ordered delivery to the callback on the receive sequence goroutine;
   the acknowledgement is published only after delivery returns
   (deliver-before-ack), so a slow or queued callback holds every later
   acknowledgement behind it. Microseconds with a sink; the NAT or tun
   on a real client.
4. The receiver's acknowledgement compression: an idle sequence
   acknowledges its first Pack at once, a streaming one at most once per
   `AckCompressTimeout`, 10 ms. A batching choice of ours, average five
   milliseconds, ten at worst; it buys about a thirty-fold reduction in
   acknowledgement frames on a stream.
5. The acknowledgement half of the path, plus the receiver's transport
   writer, where the ack frame queues behind the receiver's own sends;
   in the fixture, the 20 ms delay element, and whatever that element
   does when a second acknowledgement arrives before the first has left.
6. The sender's acknowledgement handoff and the loop's wake on
   `ackNotify`: scheduling, microseconds.

Summing the terms we know for the fixture: 20 ms of imposed delay, five
to ten of compression, about one of truncation and scheduling. Twenty-six
to thirty-one against a measured 85 to 99. The missing 55 to 70 is not in
any term the source names on an unbounded in-process wire, so it is
either in term 1, a data half that blocks the sender after the stamp, or
in term 5, a delay element that does not delay acknowledgements
independently.

### 34.3 The fixture artefact that fits the number

A delay element written as one goroutine that sleeps 20 ms per
acknowledgement is a serial line, not a delay: it forwards at most fifty
acknowledgements a second, and under 10 ms compression the receiver
offers a hundred a second, so the line backs up by ten milliseconds per
acknowledgement. The k-th acknowledgement then measures 20 + 10(k − 1)
milliseconds plus compression, the samples rise linearly across the run,
and the mean over the first ten is about 65 plus compression, near 75;
with the immediate first acknowledgement and any selective ones adding
to the offered rate, the backlog grows faster and the mean lands where
the measurement did. A delay element that arms a timer per
acknowledgement, or a per-message departure time, does not do this. The
harness should read its delay element before reading anything else.

The reading that decides it needs one line: the window already keeps a
monotonic-minimum deque of the live samples (`minimums`), and an
`RttEstimate` carrying `Min` beside `Mean` separates the cases in one
reading. A backlog reads a minimum near the imposed delay and a mean far
above it; a genuine added latency reads a minimum as high as the mean;
and the sample series, if the reader exposes it, rises across a backlog
and is flat under latency. Row: `TestRttEstimateCarriesItsMinimum`.

### 34.4 If it survives on a real path

The same two readings on the reporter's rig or the namespace cell,
beside a ping between the hosts: `Min` says the path plus fixed
processing; `Mean − Min` says queueing, ours or the window's own
bufferbloat; a mean that rises through a run says a backlog somewhere in
the acknowledgement path, which on a real path would be the receiver's
transport writer behind its inner acknowledgement stream (§31.8) or a
delay element of the rig's. If `Min` sits at the ping and `Mean` does
not, §32's second lever is real and worth what the window is worth at
no memory; if both sit at the ping, the divisor is the network's and
the window is the whole decision. The arithmetic the coordinator worked
holds either way: 2 MiB over 90 ms is 186 Mb/s and over 20 is 838, so a
factor of four and a half is on the table if the excess is ours, and
nothing in source yet says it is; one reading says.

## 35. The zombie remainder: not a fourth mechanism, but zombie resends inflating live flows' effective round trip through the shared serial stage

Three structural candidates for the reporter's threshold are eliminated
by measurement: bytes on the wire (84 Mb/s of resends against a 460 loss),
pool pinning (34 MB pinned at forty zombies, live flow 154.7 against
154.5 at zero, no fall-through), and per-flow scaling. The falsification
of the pool candidate is clean and I recorded its prediction before it
ran, so it is a real negative and not a missed regime, with the one
caveat that the cell reaches 155 Mb/s and the reporter's collapse is at
639; the boundary is simply not at 34 MB pinned on that host.

### 35.1 Per-pack service time is constant; the load on the shared stage is not

What a return pack costs to service on the send path, in work: on the
sequence goroutine, marshal, the session AEAD, resend-queue bookkeeping;
on the transport writer, websocket framing and the TLS record AEAD; then
the exchange terminates that TLS, forwards, and re-encrypts. The
sequence goroutine is per destination, so a zombie's resends do not
share it with a live client's. But everything from the transport writer
onward is shared: one carrier connection per provider per family, one
writer serializing every pack of every destination, and one exchange
ingress path. That is the serialized stage of §31.1, and its throughput
ceiling is well below the wire because it is framing plus two
encryptions on a serial path (§31.2, one to two gigabits per core split
across those passes).

The per-pack cost is constant. What scales with the zombie count is the
number of packs entering that shared serial stage per second: each
zombie is an independent send sequence resending its queue at up to
2 MiB per resend interval, so forty of them offer about 84 Mb/s of
resend packs into the one writer, on top of the live flows' data and
their return acknowledgements. A constant per-pack cost times a pack
rate that scales with population is a load on the shared stage that
scales with population, which is what the coordinator's objection asked
for and it needs no fourth mechanism.

### 35.2 Why it collapses rather than degrades: the coupling to §32

Bandwidth alone is linear and would take 84 Mb/s from a 639 provider,
not 459. The super-linear part is the coupling this program has the
pieces for. A live flow's download throughput is its window over its
effective acknowledgement round trip (§32). That round trip includes the
live flow's own acknowledgements queueing in the shared transport writer
and the shared exchange (§32.2, term 3 and term 5). When forty zombies
fill that shared writer with resend packs, every live flow's
acknowledgement waits behind them, so the live flows' effective round
trip rises with the zombie count, and since their throughput is
window/RTT against a fixed 2 MiB window, it falls as the reciprocal of a
divisor that grows with the population. That is the mechanism that turns
84 Mb/s of zombie load into 459 Mb/s of lost live throughput: the loss
is not the zombies' bytes, it is the live flows' window stranded behind
an inflated round trip. The missing 376 the backoff arithmetic could not
find (§13.6) is live throughput lost to round-trip inflation, not zombie
bytes on the wire, and that is why no bytes-on-wire accounting closed it.

The threshold shape follows: below the writer's capacity the zombies add
queueing delay roughly linearly and the reciprocal is gentle; as the
writer approaches saturation the queueing delay rises sharply and the
reciprocal collapses, which is a knee at the count where zombie resend
load plus live load crosses the shared stage's throughput ceiling. Eight,
sixteen and forty straddling that knee is what a shared serial stage
saturating looks like, and it is the same ceiling §31 and §32 name from
the other side.

### 35.3 What would confirm it, and what it is not

It is testable without a new mechanism: the instruments exist. During a
zombie sweep, read the live flows' `DestinationSendStats.Rtt` (the
reader landed in `cfc63d7`) and the transport writer's occupancy, against
live throughput. The prediction: the live flow's effective round trip
rises monotonically with the zombie count while its window is unchanged,
and live throughput tracks window over that round trip within the null
band; the knee in throughput coincides with the writer's occupancy
approaching one. If the round trip does not rise with the zombie count,
this is wrong and the remainder is a fourth mechanism after all, and what
would then distinguish it is that the live flow's window itself would
have to be shrinking, which `DestinationSendStats` and the ladder state
would show; a collapse with a flat window and a flat round trip is none
of the four and would be genuinely new.

It is not the transport parallelism candidate wearing a different coat,
though it shares a cause: parallelising the writer across several carrier
connections per client (§31.3) would relieve exactly this, because the
zombie resends and the live acknowledgements would no longer serialize
through one writer, which is a second reason that candidate is worth its
place beside lanes. And it is not per-flow scaling: the zombies are per
destination, and it is their aggregate resend load on the shared stage,
not any per-flow cost, that does it. The cheapest mitigation is the one
this program already built for a different reason: the abandon timeout
(§10) retires a zombie's sequence in 120 to 150 s, which removes its
resend load from the shared stage, so §10's benefit is not only the
retired flows but the round-trip inflation it lifts off every live flow
of the provider, which is the reporter's 72 per cent restored. That
reframes §10 as a throughput fix for the live flows and not only a
cleanup of the dead ones.

## 36. The plateau: an inner-path window at four mebibytes, the framed-to-goodput factor derived, and what the delivery-sized rule converges to

The constant-queue sweep at 200 ms and 400 ms confirmed §32's shape and
refuted §32.1's naming of the next ceiling. The harness fits all four
constant arms with an effective window of the smaller of 0.845 times the
nominal queue and 3.91 MiB, in goodput bytes over the imposed delay,
latency-invariant to half a per cent across the doubled round trip. Two
terms, both derivable, neither of which is the H3 stream window.

### 36.1 The calibration that decides it without a fit

The resend queue charges `len(transferFrameBytes)` per item
(`transfer.go:9721–9726`): the nominal queue is a framed-byte window.
The harness measures inner goodput. So a 2 MiB queue that delivers
0.845 × 2 MiB of goodput per round trip is telling us the sweep's own
conversion from a framed window to goodput times delay, and that
conversion is below one for two reasons that never go away: every framed
byte carries less than one payload byte, and the loop's round trip is
the imposed delay plus our own acknowledgement delay. The plateau's
3.91 MiB is in the same goodput units. A framed window of 3 MiB
(3,145,728 B) cannot put 3.91 MiB (4,100,000 B) of goodput in flight
over a round trip at least as long as the imposed delay, because
goodput per framed byte is below one. That excludes the 3 MiB H3 stream
window as the binder before any fit and at any memory budget, since
`memoryTargetScale` returns one at or above the reference budget and a
fraction below it (`memory_budget.go:79–84`): the scale can only lower
that ceiling. §32.1 is amended accordingly.

### 36.2 Where the plateau is not, and the H3 prediction that stands

The coordinator read the cells' source: the abandon fixture builds send
and receive gateway transports fed by Go channels, with the delay
element on that wire, and the zombie and flow-scaling cells inherit it.
The only real socket in any of the three is the loopback origin the
provider dials. There is no QUIC stream window and no carrier TCP socket
on the measured path, so the two caps §32.1 placed beside the queue
were not there to bind.

The H3 finding stands on its own as a prediction for the namespace cell,
which will have a real carrier. From source:

- `transport.go:684–687`: stream window 256 KiB initial,
  `MemoryScaledByteCount(mib(3), kib(384))` maximum; connection window
  512 KiB initial, `MemoryScaledByteCount(mib(4), kib(512))` maximum.
  `transport.go:858–889` passes them unclamped into `quic.Config`.
- `transport.go:2695`: one `OpenStreamSync` per H3 connection, so the
  stream window binds before the connection window.
- quic-go v0.61 (`flow_controller_base.go:55–75`) doubles the window
  whenever more than half of it is consumed within four times that
  fraction of the smoothed round trip since the epoch began, capped at
  the maximum, and sends `MAX_STREAM_DATA` once a quarter of the window
  is consumed (`WindowUpdateThreshold = 0.25`). Growth is path-driven;
  the ceiling is a fixed, memory-scaled byte count with no path in it.
  The same defect shape as the resend queue, one layer down.
- It is the receiver's setting on each hop. Where the path runs through
  the platform, the client-to-platform and provider-to-platform
  directions terminate in the server's quic-go configuration, which is
  in the server tree and not in this file.

Prediction for the namespace cell, stated before it runs: on H3 with
the queue above 3 MiB and the delay on the carrier hop, the plateau
sits at 3 MiB of framed bytes per round trip, which at the 0.865
framing factor of §36.3 is 2.6 MiB of goodput times delay, 109 Mb/s at
200 ms, below the 164 measured in process; halving
`H3MaxStreamReceiveWindowByteCount` on the receiving endpoint of the
delayed hop halves it. If the cell's transfer frames ride the H3
datagram path instead (`UseDatagramForPath`, `transport.go:3251`),
stream flow control does not apply and the plateau is this section's
inner window again.

### 36.3 The 0.845 factor, derived rather than fitted

The factor is the product of two things: goodput per framed byte, which
is a property of the wire format and the inner MTU, and the ratio of the
imposed delay to the loop's actual round trip, which is our
acknowledgement delay. Both are stated below; the record already
carries the two numbers that make the first exact.

Bytes of a full-size upload packet on the wire, as a running ledger,
from the protobuf definitions (`protocol/transfer.proto`,
`protocol/frame.proto`) and the send-path literal
(`transfer.go:8366–8404`), steady state, one IP packet per Pack
(`sendPackBatchMaxMessageByteCount = DefaultMtu = 1100`, so two full
packets never coalesce). The harness read an earlier form of this list
as 1,375 B, which gives 0.901; that reading omitted the frame wrapper
and the seal, so the ledger is stated with totals at every step:

    inner IP packet, DefaultTunnelMtu                        1,280
      of which TCP payload: 1,240, or 1,228 with timestamps
    Frame{message_type 2, message_bytes tag+len 3, raw 2}    1,287
    as Pack.frames, repeated tag + two-byte length            1,290
    Pack: message_id 18, sequence_id 18, sequence_number 4,
      tag{send_time} 9                                        1,339
    outer Frame{TransferPack}: message_type 2, tag+len 3      1,344
    TransferFrame.frame tag + length                          1,347
    TransferFrame.transfer_path, two ids                      1,385  plaintext
    seal: nonce 12, GCM tag 16, field tag+len 3,
      session_role 2, session_companion 2                     1,420  encrypted

`head`, `nack`, `contract_frame` and `contract_id` are absent on a
steady-state acknowledged Pack; a compact contract head adds 18 and a
stream id 18. So goodput per framed byte is 0.887 plaintext and 0.865
encrypted with timestamps (0.895 and 0.873 without); for download,
where the provider packetizer builds 1,100 B packets with 1,060 B of
payload, 0.880 and 0.855.

Measured: 0.860, from the record's mean frame length against the inner
segment size, half a per cent from the encrypted-with-timestamps row.
It implies a mean frame of 1,428 B, eight bytes above the ledger, which
is a varint or an occasional contract id and is within what the ledger
can say. The fitted 0.845 is that factor times D/(D + δ) with δ our
acknowledgement delay: 0.860 gives δ = 3.5 ms at 200 ms, under the
10 ms `AckCompressTimeout` and near its mean wait. The plaintext row
would need δ = 10 ms, every acknowledgement waiting the full timer,
which the timer does not do; the two 2 MiB points solved directly for
the pair gave 0.84 and 4 to 5 ms, the same reading.

What makes it exact rather than argued: `DestinationSendStats` already
carries `writeByteCount` and `writeCount`, whose quotient is the mean
framed bytes per item, and the origin socket's `TCP_INFO` carries the
inner segment size. Their quotient is the factor; the residual against
0.845 is δ, which is itself a reading of our acknowledgement path and
should be recorded as one.

### 36.4 The 3.91 MiB ceiling: the tun's send buffer, not a kernel socket

With δ = 4.7 ms the plateau is 3.91 MiB × (204.7/200) = 4.00 MiB of
payload in flight at 200 ms and 3.96 MiB at 400 ms: a 4 MiB
payload-counted window at full utilisation. A framed-counted window
would have to hold 4.6 MiB, and nothing on the in-process path is
configured to that.

Which 4 MiB is decided by which loop the delay is on, and I had that
wrong when I first listed the candidates. The delay element sits on the
transfer wire between client and provider. The origin's kernel socket
and the provider's upstream socket close a different loop, provider to
origin over loopback, whose round trip is microseconds; a 4 MiB send
buffer over microseconds bounds nothing, and the kernel acknowledges
into its receive buffer whether or not the NAT is reading, so those
buffers are passive holds behind the NAT, not windows over the delayed
wire. The windows that do close a loop across the delayed wire are the
transfer sequence's, which the sweep raised past the plateau, and the
inner TCP's: on upload the client's gVisor send buffer, gVisor's
congestion window, and the provider ladder's advertised window; on
download the client's gVisor receive window.

The tun's buffers autotune. `tun.go:221–235` sets the receive and send
ranges, `Default` `MemoryScaledByteCount(mib(1), kib(128))` and `Max`
`MemoryScaledByteCount(mib(4), kib(512))`, and does not touch
`TCPModerateReceiveBufferOption`, whose stack default is on
(gVisor `tcp/protocol.go:613`), so receive-side moderation runs
(`endpoint.go:916–918,1322–1334`), sized from bytes copied per measured
round trip. The send buffer grows to twice the congestion window times
the segment size, capped at `Max` (`endpoint.go:3446–3475`). Both grow
from the path and stop at a constant. On a lossless wire the congestion
window is unbounded, so the send buffer reaches its 4 MiB cap and binds
in-flight payload at 4 MiB: 4.00 MiB measured. The ladder's maximum is
1 MiB under `DefaultTcpBufferSettings` (`ip.go:421`), which would have
capped an upload cell at 42 Mb/s at 200 ms, and 16 MiB under the
buffer-size settings (`ip.go:462`); the cell therefore uses the latter,
and the record should say so.

The discriminator, now sharpened: at fixed 200 ms, halving
`TcpSendBuffer.Max` on the client halves the upload plateau, 164 → 82
Mb/s; halving `net.ipv4.tcp_wmem[2]` in the origin's namespace moves
nothing, because that socket is not on the delayed loop. A download
cell plateaus at the same 4 MiB through the receive side's moderation,
and halving `TcpReceiveBuffer.Max` halves it. The ratio of goodput
times delay to the halved window sits near one in every case, since
these windows count payload.

If that ratio sits near 0.845 instead, something on the path is
charging framed bytes and the tun is not the binder. On an in-process
carrier there is exactly one place that can be: the fixture's wire, if
its capacity is bounded in items rather than bytes, a buffered channel
depth or a per-message goroutine budget in the delay pump. An item bound
has a signature the record can show without a new cell: the count of
frames in flight is the same at both delays, 129.5 Mb/s over 1,420 B
frames for 200 ms is 2,280 frames and 65.1 Mb/s over 400 ms is 2,290,
and `writeCount` less the acknowledged count would sit at that
capacity. The other framed-byte bounds are excluded by the sweep
itself: the resend queue was raised past the plateau, the shared resend
budget is sized as one `ResendQueueMaxByteCount` and scales with it,
and the 2.5 MiB receive queue holds only out-of-order Packs.

Confirmed. Cell C identified the plateau as the tunnel's send buffer at
its 4 MiB cap, with both predictions above hitting and 32 of 32 runs
valid: halving the tun's maximum halved the plateau, halving the
origin's socket ceiling moved nothing.

### 36.5 The delivery-sized rule as built

The rule that produced the first measured speedup is, from
`transfer.go:8836–8873`:

    Interval = rttWindow.ScaledRtt()
    Window   = clamp(scale × deliveredBytesOver(Interval), floor, ceiling)

and `ScaledRtt` is `clamp(RttScale × mean, RttMinResendInterval,
MaxResendInterval)` (`transfer_rtt.go:308–326`) with `RttScale` 2.0,
the floor 300 ms and the ceiling 8 s (`transfer.go:691–709`). So the
"last acknowledgement round trip" of §32.5 is twice the mean round
trip, and never less than 300 ms. With `scale` 2 the window is four
times what the path delivered per round trip on any path whose round
trip exceeds 150 ms, and on any shorter path it is twice what the path
delivered in 300 ms, which at a 25 ms round trip is twenty-four times
the bandwidth-delay product. The doc comment's safety argument, at most
one round trip of extra queueing and therefore a scale of two, is
written for a rule that multiplies by one round trip; this one
multiplies by two, or by 300 ms.

That is what the harness saw: 4 × 3.91 MiB is 15.6 MiB, the 14 to 16 MiB
the rule computed at 200 ms. The ceiling was reached by construction,
not by a runaway, and it was inert for throughput because the inner
window bounds what the inner stack can hand the sequence: the transfer
window can only fill with Packs the inner TCP has sent, and it sends at
most 4 MiB unacknowledged. Where the excess would not be inert is a path
whose binder is below the sequence rather than above it, a slow carrier
hop or the writer's own service rate: there the inner stack keeps
sending, the queue fills to the whole transfer window in front of the
slow stage, and every flow sharing that stage pays window over rate of
added round trip, which is §35's mechanism turned on ourselves.

The mean is the wrong multiplier for a second reason. The tag is
stamped at Pack construction (`transfer.go:8382,8542,8644`), ahead of
the transport writer, so the sample contains whatever queue the window
itself creates. A rule that sizes from rate times a round trip that
grows with its own window has the ceiling as its only fixed point on a
path bound below it. `RttEstimate.Min` exists (`transfer_rtt.go:227`),
taken from the monotonic-minimum deque under the same lock and coalesce
as the mean; the minimum is the propagation plus our fixed delays and
does not grow with the window, and rate times that minimum has the
plateau as its fixed point. That is the change §36.7 and §37 make.

### 36.6 The ramp: where the hundred milliseconds comes from, and the honest trade

Solved from two transfer sizes, the adaptive arm reaches the same steady
state as a constant 16 MiB queue, 164.7 against 164.1 Mb/s, and pays
about 100 ms more getting there, which is why the multiple grows with
transfer size, 1.7 at 16 MiB and 2.1 at 64. The cost is structural, not
a matter of distance. Growth needs evidence, and evidence is
acknowledgements: a window raised at t is not seen delivering until its
first acknowledgements return at t plus one round trip, and a trailing
sum over a full interval does not read the new rate until a further
interval has passed. Two round trips per doubling, of which the first is
physics and the second is the estimator's shape. With the binder at 4
MiB of payload, 4.6 MiB framed, and a 2 MiB floor, the ramp is one
doubling; a higher floor would buy almost nothing here and would cost
what §15 measured on a 32 MiB budget.

The trades, stated so a campaign can pick and nobody guesses:

- Rate over a sub-interval, projected. Replace the trailing sum over
  the interval with a rate, bytes acknowledged between two ring samples
  over the time between them, times the minimum round trip. The
  estimator reads the new rate as soon as the raised window's first
  acknowledgements arrive, which removes the second round trip per
  doubling and leaves the first. The bound on over-grant is unchanged:
  the window is still k times a demonstrated delivery. The new cost is
  projection from a short interval: acknowledgements arrive in bursts
  under the receiver's 10 ms compression and the ladder's 50 ms, and a
  rate read across too few of them over-projects by the burst ratio.
  The interval must span several compression periods; the ring's
  present cadence, a sample every `RttMinResendInterval / 4` = 75 ms
  (`transfer.go:8774`), is already coarser than that and coarser than a
  25 ms round trip, which is a second reason the sum-over-horizon form
  cannot serve a short path: `deliveredBytesOver(25 ms)` returns up to
  100 ms of delivery.
- A larger k. Removes ramp time in proportion and multiplies the
  over-grant in a stale-estimate episode by the same factor: k − 1
  round trips of queue in front of whatever binds, paid by every flow
  sharing the writer. The property built in was one round trip. Nothing
  here argues for spending it.
- A higher floor, or an evidence-free start. Grants without
  demonstration. §37.5 makes this a deliberate, written-down bet rather
  than an inherited floor, and says which direction of error is cheap.

The first is the design-consistent one; it changes no bound. The second
trades the margin and should be measured against it, not adopted for
the ramp. The third is where the composite design puts the startup
cost, deliberately.

### 36.7 Whether to stop at the plateau: yes, and the interval is the mechanism

The question was whether the rule should stop climbing when additional
window stops producing additional delivery, rather than converging
toward a configured number the path cannot use. It should, and no
detector is needed: a rule of the form k × rate × minimum round trip
stops when the rate stops, because nothing else in it moves. The
plateau becomes the fixed point, at k times the path's delivery per
propagation round trip, and the configured ceiling becomes what it
should be, a budget bound that a well-behaved path never reaches. With
the mean or with `ScaledRtt` the fixed point is the ceiling, on every
path, and the "plateau detection" being asked for would be a patch over
the wrong multiplier.

On a composite tree (§37) the same form answers the harder version of
the question: delivery not responding may mean the layer above has not
grown yet rather than that the path is full. k × rate × minimum round
trip holds at k times whatever the layer above lets through, and
follows it up one round trip after it grows. The rule never needs to
know which it was.

What remains after that change is k itself at steady state: k × BDP of
window is (k − 1) × BDP of standing queue in front of the binder, one
round trip of it at k = 2, which is the margin that keeps the pipe full
through an estimate that runs briefly short and is also latency every
sharer of the writer pays. If the standing queue proves costly in the
zombie sweep's live-flow round trip, the shape that removes it is the
known one: probe with k above one, drain, cruise near one, which is
BBR's gain cycle. That is a candidate after the minimum-round-trip
change is measured, not before, and the campaign picks k.

Predictions, stated before the cell: at 200 ms the computed window
falls from 14–16 MiB to about 9.3 MiB, twice the 4.6 MiB framed binder,
with steady-state throughput unchanged at 164 Mb/s and the startup
excess halved to about 50 ms; at a 25 ms imposed delay the computed
window is twice the rate times 25 ms and doubles per round trip to the
binder instead of starting at the ceiling. If throughput at 200 ms
falls with the smaller computed window, the inner binder is not what
§36.4 says and the transfer window was doing work above 4.6 MiB, which
the record's `Rtt.Min` against `Rtt.Mean` would show as a round trip
that had been growing with the window.

## 37. Every window from the origin socket to the client application, and the composite fix: a target throughput, a memory budget, and an initial size

The user has settled the scope: every buffer in the path sizes from the
round trip, and the configuration is a target throughput, a memory
budget, and a reasonable initial size, from which each window is
derived rather than configured. This section is the enumeration that
design needs, the restatement of every constant as the target and round
trip it silently encodes, the sort into layers that already autotune
and layers that do not, the round trip each layer can actually measure,
the composite rule, its memory bound for one flow and for a provider at
scale, and an implementation order in which each step is measurable on
its own. Our cell is the reference throughout: a ceiling is a window
over a round trip, so the cell reaches any regime by moving the round
trip, which is what produced the confirmed result, and every binding
claim below is testable by halving one window at a fixed delay.

### 37.1 A window means nothing without its loop

Bytes in flight are bounded by a window only relative to the
acknowledgement loop that window closes, and the path has four loops
with four different round trips:

- Loop A, provider to origin: a kernel TCP connection from the NAT's
  upstream socket to the origin. Its round trip is the real network to
  the origin, microseconds over loopback in the cell.
- Loop B, the inner TCP: the client's stack to the provider's NAT
  (`TcpSequence`), which terminates it. Its round trip is the whole
  tunnel, the carrier crossed twice, plus every Transfer queue and
  acknowledgement delay in both directions. It is the longest loop.
- Loop C, the Transfer sequence: per destination, client to provider
  through the platform, with the acknowledgement returning over the
  same carriers plus the receiver's 10 ms compression. Its round trip
  is the carriers' plus our delays.
- Loop D, the carrier per hop: client to platform and platform to
  provider, each an H3 connection or an H1 TCP socket with its own
  windows, over that hop's network round trip alone.

Loops A and D are short in the cell and in the datacenter regime;
loop B contains loop C, which contains loop D. The same bytes sit in
all of them at once, which is what §37.7 is about.

### 37.2 The enumeration

For each window: its value and derivation, whether that derivation
carries a term from the path, the loop it closes and the estimator it
has, the target and round trip the constant encodes, and where it sits
in the binding order. Constants are restated in goodput bytes per round
trip, framed layers converted at the 0.865 of §36.3, so that layers can
be compared at all; the rate columns are that quantity over 25 ms and
over 200 ms, the two regimes the cell can impose.

Transfer, loop C, both directions:

- C1, the send window, `ResendQueueMaxByteCount`,
  `MemoryScaledByteCount(mib(2), kib(256))` (`transfer.go:762`): 2 MiB
  framed, 1.73 MiB goodput per round trip. No path term as shipped; the
  delivery-sized rule adds one, as built through `ScaledRtt` (§36.5).
  Estimator: `RttWindow`, mean and minimum, sender's clock. Encodes
  580 Mb/s at 25 ms, 72 at 200 (measured 68.6). First binder in the
  cell, measured.
- C2, `SequenceBufferSize`, 32 items: the pre-send burst buffer in
  items (§32.1). Not a window; unchanged by this design.
- C3, the receive hold, `ReceiveQueueMaxByteCount`,
  `MemoryScaledByteCount(mib(2) + kib(512), kib(320))`
  (`transfer.go:831`): 2.5 MiB framed, 2.16 MiB goodput. No path term;
  no estimator, and none possible, since a receiver sees arrivals and
  not a round trip. Encodes 725 Mb/s at 25 ms, 91 at 200, but only
  under loss: on an in-order path it is empty. When an arrival does not
  fit, later items are evicted to admit an earlier one and an arrival
  above everything held is dropped and counted
  (`transfer.go:11789–11800`, `ReceiveQueueDropCount`). Position: the
  loss-regime binder, §37.3.
- C4, the shared budgets, `ResendQueueBudget` and `ReceiveQueueBudget`
  with `NewTransferMemoryBudget`, and the lane pools sized as one
  `ResendQueueMaxByteCount` (§27, §29): caps that scale with C1 and
  C3, no path term of their own; the place the composite budget already
  has a foothold.

The inner TCP, loop B:

- B1, the tun's send buffer, `TcpSendBuffer{Default 1 MiB, Max 4 MiB}`
  memory-scaled (`tun.go:93–106`), upload. Path term: yes, twice the
  congestion window times the segment size, capped at `Max`
  (`endpoint.go:3446–3475`). Estimator: gVisor's own, loop B. Encodes
  1,340 Mb/s at 25 ms, 168 at 200 (measured 164). Second binder in the
  cell, measured; the constant is the cap only.
- B2, the tun's receive window, `TcpReceiveBuffer{Default 1 MiB, Max
  4 MiB}`, download. Path term: yes, receive moderation on by default
  (`protocol.go:613`), grown from bytes copied per measured round trip
  (`endpoint.go:1322–1334`). Same numbers as B1 for the other direction.
- B3, gVisor's congestion window: path-sized, no constant; binds only
  under loss.
- B4, the ladder's advertised window, upload, `MinWindowSize`,
  `InitialWindowSize` and `MaxWindowSize`: 1 MiB under
  `DefaultTcpBufferSettings` (`ip.go:421`), 16 MiB power-of-two-scaled
  under the buffer-size settings (`ip.go:462`). Path term: yes but from
  the wrong loop, it doubles while `writePayloads` does not block and
  halves when it blocks half the time, which is loop A's backpressure
  and carries no round trip of loop B. No estimator of loop B; the NAT
  has none. Encodes 335 Mb/s at 25 ms and 42 at 200 as shipped plain,
  5,370 and 670 with the buffer-size settings. Above B1 in the cell;
  below everything at the plain default, which is the single most
  inconsistent constant in the chain.
- B5, the NAT's download send hold: the `DataPackets` awaiting the
  client's inner acknowledgement, bounded only by B2, the client's
  advertised window, through `receiveAckCond.Wait()` (`ip.go:5287`).
  No cap of the provider's own, no estimator. Encodes whatever the
  client advertises; forty clients at 4 MiB is 160 MiB the provider
  did not choose.

The carrier, loop D, per hop, the receiver's setting on each:

- D1, the H3 stream window, 256 KiB initial to
  `MemoryScaledByteCount(mib(3), kib(384))` (`transport.go:684–685`):
  3 MiB framed, 2.6 MiB goodput. Path term: yes, quic-go's growth
  (§36.2); the ceiling is the constant. Estimator: quic-go's smoothed
  round trip of its hop. Encodes 870 Mb/s at 25 ms, 109 at 200. Binds
  before B1 on a real carrier with the queue above 3 MiB; absent in
  process. The server's side of two hop-directions is in the server
  tree.
- D2, the H3 connection window, 512 KiB to
  `MemoryScaledByteCount(mib(4), kib(512))`: 3.46 MiB goodput; one
  stream per connection (`transport.go:2695`), so inert behind D1.
- D3, the H3 UDP socket buffers, `H3SocketReadBufferByteCount` and
  `H3SocketWriteBufferByteCount`, 1 MiB memory-scaled: not windows,
  there is no loop through a UDP socket; they absorb bursts of rate
  times scheduling latency and overflow as loss. A different rule, not
  this design's.
- D4, quic-go's congestion window: path-sized; its packet-count
  constant is far above any window here.
- D5, the H1 carrier's kernel socket: `tcp_wmem[2]` 4 MiB and
  `tcp_rmem[2]` 6 MiB stock, autotuned, path term yes, ceilings the
  host's sysctls; no pin on the carrier dialer today. Encodes 1,340
  and about 1,600 Mb/s at 25 ms per hop.

Provider to origin, loop A:

- A1, the provider's upstream socket, `ConnectSettings.DialControl`
  from §15: pinned at twice `min(MaxWindowSize, wmem_max)` when that
  exceeds the autotune ceiling, otherwise autotuned to `tcp_wmem[2]`
  and `tcp_rmem[2]` with the kernel's own estimator. Path term: yes.
  Encodes about 1,340 Mb/s at 25 ms of loop A's round trip; inert in
  the cell, the binder for a distant origin in production.
- A2, the origin's own socket: not ours.

Buffers that bound items or bursts and not bytes in flight, listed to
close the enumeration and left alone: the receive-side handoff of 256
items and its adaptive pack handoff, the NAT's 64 KiB read chunk and
`WriteBatchSize` 64, the coalescer's fixed frame array.

Read across, the implied targets at one round trip run from 42 Mb/s to
5,370 at 200 ms depending on which constant one asks, and no two layers
agree. That disagreement is the binding order: at equal round trip it
is C1 (1.73 MiB goodput), then C3 under loss (2.16), then D1 (2.6),
then B1 and B2 (4.0), then D2, then B4 with the buffer-size settings
(16), and the plain B4 (1 MiB) below all of them. The cell measured
C1 then B1, which is that order with D absent.

### 37.3 The receive hold under loss, and whether both must move

The coordinator's question: if the send window can now grow to 16 MiB
while the receive side holds 2.5, a loss on a fast path has a hole it
cannot fill. It is real. A Pack lost at sequence n with W bytes in
flight behind it puts up to W − 2.5 MiB of arrivals above the hold; each
is dropped on arrival (`transfer.go:11789–11800`) and must be sent
again after the sender's gap recovery or its paced interval, which is
floored at 300 ms. One loss then costs one window of retransmission,
13.5 MiB at 16 MiB, on top of the hole's own recovery; at the cell's
plateau rate that is two thirds of a second of resending per loss
event, and the acknowledgements those drops destroy are the ones
FLIGHTGATEFIX §34.2 identified as what a lane proof depends on. TCP
cannot have this failure because its receive buffer is its advertised
window; Transfer has no receiver-advertised window at all (§32.1), and
that is the missing coupling, not a second window to size.

So the answer is not that both must be sized from the path. The hold
has no round trip to size from. The answer is that the sender's window
may never exceed what the receiver will hold, and the receiver must say
what that is: `Ack` gains a `receive_window_byte_count`, the receiver's
current hold capacity, and the sender clamps its window to the latest
advertised value; absent, a legacy peer, the sender keeps today's
constant. The hold's capacity is then a budget quantity, the receiver's
share of §37.4's memory, and it costs nothing on a clean path because
the hold is empty there. The `Ack` already carries `selective` per
message (`transfer.proto:148–154`), so the receiver already tells the
sender what it holds; this adds what it could hold.

### 37.4 The configuration surface: what each constant actually was

Every constant in §37.2 is a target throughput at an assumed round
trip, and the defect is that both are implicit and neither travels with
the path. Two mebibytes is 500 Mb/s at 33 ms or 84 at 200; three on the
H3 stream is the same thing with a different assumption. The memory
scale makes it worse in a specific way: scaling a byte count by memory
keeps the implied target fixed only if the round trip never changes, so
a phone at a 32 MiB budget gets half the window and, on the same path,
half the target, which nobody chose.

The surface the user has set is three quantities, and each is a thing
an operator can reason about without knowing the round trip:

- a target throughput, T, in goodput;
- a memory budget, M, which the process already has
  (`SetMemoryBudget`, `memory_budget.go:56`, with
  `memoryTargetScale`) and which the carriers already draw on
  (`PlatformTransportBudget`) and Transfer already draws on
  (`ResendQueueBudget`, `ReceiveQueueBudget`);
- an initial size, the bet a layer makes about the path before it has
  measured it, which §37.5 argues is a third configured thing and not
  derived from the other two.

From these each layer's window is derived:

    window_L = initial_L                                  until sampled
    window_L = clamp(min(T × rtt_L, k × achieved_L × rtt_L),
                     floor_L,
                     share_L)                             once sampled

where `rtt_L` is the minimum round trip that layer measures on its own
loop, `achieved_L` is the delivery rate it measures, `share_L` is its
draw on M, `floor_L` is a working minimum of a few packets and not the
initial bet, for the reason §37.13 gives, and framed layers divide by their goodput factor so that a
goodput target means the same bytes at every layer. The three terms are
three visible regimes: if the path is full the window sits at k times
delivery, §36.7; if the target is the limit it sits at T times the
round trip; if memory is the limit it sits at the share and the achieved
rate falls short of T, which is the honest failure and is visible in
the estimate's fields rather than silent in a constant.

Per role or per process. One target per process, chosen by role, the
way the memory budget already has role profiles ("provider entry points
select their explicit profile", `ip.go:418–420`). Per-layer targets
would recreate the inconsistency of §37.2 by hand; the layers are a
serial chain carrying the same bytes, and one intended rate is the only
thing that makes their windows comparable. On a provider T is per
client, the rate one client's flows may take, and M caps the aggregate;
on a phone T is the process's.

Layers that already autotune keep their growth and get their ceiling
from the surface. That is the smaller change and the better one: the
mechanism that grows quic-go's window from 256 KiB, the tun's buffers
from 1 MiB and the kernel's from its defaults is already the k ×
achieved term of the rule above, measured by the layer that owns the
loop; what each lacks is a ceiling that is not a constant and an
initial size that is not one either. For those layers the composite
sets `initial_L` and `min(T × rtt_L, share_L)` and touches nothing
else. quic-go is the precedent in the tree: an initial size, a growth
mechanism and a ceiling as three separate concepts, defective only in
that the ceiling is a constant. Transfer had none of the three, which
is why §32.5 had to build a rule; the NAT has growth from the wrong
loop and no estimator, §37.6.

Convergence. T × rtt_L is not a number anyone should allocate against
until rtt_L is worth trusting. Until it is, the window is `initial_L`,
and the estimate carries its own trustworthiness: `RttEstimate` already
reports `SampleCount` and `NewestSampleAge` (§33), and the rule uses
the measured terms only above a sample count and below an age that the
campaign picks. The initial size is what covers the gap, which is why
it is first-class.

### 37.5 The initial size

What it derives from. A fixed byte count per layer, or the target times
an assumed round trip that is written down. The second, for the reason
the user gave: it is exactly what every current constant already is,
except undocumented, and writing the assumption beside the target makes
it reviewable. It also makes the assumption one number: every layer's
initial size is T times the same assumed round trip, converted by that
layer's goodput factor, so that no layer starts smaller than the others
and the ramps run concurrently rather than in series. That last point
is the composite's answer to the ramp: on a tree where every layer
climbs, the startup cost is the sum of serial ramps if each layer waits
for the one above to grow before it can see delivery, and the maximum
of them if they all start at the same bet. The assumed round trip is
per role, since a datacenter provider and a phone on a cellular path
are not betting on the same thing, and the campaign picks it.

Which way to be wrong. Too small costs a ramp: one round trip per
doubling from the initial size to the path's window, which is
measurable and bounded, and on the cell is the 100 ms of §36.6. Too
large costs, where the window is credit rather than backed memory, a
standing queue in front of any layer below that turns out slower, which
is latency for every flow sharing the writer (§35), and where it is
backed, memory held for nothing on a short path, which on a phone is
the expensive direction. The asymmetry argues for a bet on the short
side and a climb, from the argument and not from the current
behaviour: the cost of a low bet is one measurable round trip per
doubling and nothing else, and the cost of a high bet lands on other
flows and on the device. The ramp is then addressed by the concurrency
of the bets and by the estimator's form (§36.6), not by betting high.

### 37.6 The round trip each layer measures, and the layers with none

- Transfer send: `RttWindow`, mean and minimum, on loop C. The
  minimum is the multiplier (§36.5); the mean and the gap between them
  are diagnostics.
- Transfer receive: none, and none needed, §37.3; it advertises a
  share.
- The tun's send and receive: gVisor's own estimator on loop B, the
  longest loop, which is why B1 and B2 bind earlier than their byte
  rank suggests once the hops have different round trips: 4 MiB over
  the whole tunnel against 3 MiB over one hop.
- quic-go: its own smoothed round trip on its hop, both sides.
- The kernel: its own, loops A and D.
- The NAT: none. The ladder sizes from loop A's backpressure, and the
  download hold from the client's advertisement. The design does not
  give it an estimator. For upload, the client's stack already sizes
  from loop B, so the NAT's advertised window needs only to be no
  smaller than the client's send window, which makes it the same kind
  of thing as the Transfer receive hold: a share, advertised. Its
  `InitialWindowSize` becomes the initial bet and its `MaxWindowSize`
  the share; the doubling-and-halving on backpressure stays, because it
  is the one thing on the path that carries loop A's state into loop B
  and it is right to. For download, the NAT sends no more than the
  smaller of the client's advertised window and the provider's share:
  a sender may always send less than it is offered, and today it has no
  bound of its own, which is B5.

The principle that falls out, and it is one principle: sizing lives at
senders, who can measure a round trip; receivers advertise a share of
memory. That is TCP's own design, and the chain's defects are the
places it is not followed: Transfer's receiver advertises nothing, the
NAT's download side has no share, and every ceiling is a constant.

### 37.7 Memory: the compounding, and the composite bound

The same bytes are held in more than one place because there are three
reliable layers, each keeping a retransmission copy: on a provider
serving a download, the NAT's `DataPackets` until the inner
acknowledgement, the Transfer frame until the Transfer acknowledgement,
and the carrier's copy until the carrier's, quic-go's stream data or
the kernel's socket buffer. Three copies at the sender; at the
receiver, one, the tun's receive buffer until the application reads,
plus the Transfer hold under loss. And a fourth on loop A, the upstream
socket's kernel buffer, which is the kernel's memory but the provider's
host.

What is held is not what is permitted, and the record now has the
measurement that separates them (§37.13): a 16 MiB window on a short
path held 1.04 MiB of pool, the same as a 2 MiB window on the same path
held, because a window is an admission limit and occupancy is what the
path puts in flight. Occupancy at a layer is the achieved rate times
that layer's own round trip, plus whatever stands as queue when the
layer below is slower; it reaches the permission only where the path
can fill it. One flow on a long path therefore holds, at the sender,
the achieved rate times the sum of the loops' round trips over the
copies, `achieved × (rtt_B + rtt_C + rtt_D)`: at most three times
`T × rtt_B`, about 1.75 times it in the production ratio of §37.11,
and far less than any of those on a short path however large the
window. Sizing every layer to its own bandwidth-delay product does not
change the copy count; it was already three, at three constants that
happened to be near each other. What the budget does is make the
aggregate a choice: it bounds occupancy, the sum of what clients
actually hold, and when that sum approaches M admission stops at the
pool and the achieved rate falls short of T, visibly. The composite
bound is then

    one flow, occupancy:   Σ over copies of achieved × rtt_L,
                           at most Σ over copies of min(T × rtt_L, share_L)
    a provider:            Σ over clients of occupancy ≤ M, by construction

and the permissions may sum to more than M, because on every path
shorter than the knee they are not held. A memory argument that counts
permission as occupancy would push the shares smaller than they need
to be and cost throughput on exactly the paths that could use it,
which is why the earlier form of this paragraph was wrong in the
direction that matters.

Two consequences worth having plainly. First, sizing from the path
with a budget is cheaper than today's constants, not dearer: a flow on
a short path holds T × rtt, which is below the constant whenever the
round trip is below the constant's hidden assumption, and today it
holds the constant regardless. Second, the copy count is the lever the
budget cannot reach. The NAT's held segment and the Transfer frame that
carries it can be one buffer, the frame's payload referencing the held
segment, which takes the sender from three copies to two and is worth
a third of the provider's memory at any budget. It is a follow-on to
this landing and is named here so that it is not mistaken for part of
it.

### 37.8 "Size the binder and cap the rest just above it"

The cheaper fix is unsound across regimes, and the record already
contains the counterexample. Which window binds is the minimum over
layers of its goodput bytes over its own loop's round trip, and those
round trips differ per layer and change with the path: the H3 stream
window sat above the Transfer queue at the shipping constants and below
it the moment the queue was sized from the path; it moves again when
the delay sits on the origin leg and loop A binds; and B1 outranks D1
whenever the tunnel's round trip is more than four thirds of one hop's.
A cap set "just above" one regime's binder is the binder in the next.

What survives of the idea is its economy, and the composite keeps it:
one target, one budget and one assumed round trip give every layer a
consistent ceiling by construction, the measured round trip is the
only thing that differs per layer, and the layers that already grow
themselves are not given new sizing at all. That is cheaper than four
independent rules and does not fail when the path changes.

### 37.9 What each layer becomes

- Transfer send, both roles: `initial` until sampled, then
  `window = clamp(min(T × rtt_min / f, k × achieved × rtt_min),
  floor, min(share, advertised))` with `floor` a few packets, with
  `rtt_min` from `RttEstimate.Min`, `achieved` as a rate between ring
  samples (§36.6), `f` the goodput factor, `advertised` from §37.3.
  `DeliverySizedWindowScale` and `DeliverySizedWindowCeilingByteCount`
  are replaced by the target, the assumed round trip and the share.
- Transfer receive, both roles: hold capacity from the share;
  `Ack.receive_window_byte_count`; drops at the hold become impossible
  by construction for a peer that reads the field.
- The tun: `Default` becomes the initial bet, `Max` becomes
  `min(T × rtt_assumed_max, share)` where `rtt_assumed_max` is the
  longest path the target is meant to hold on, written down; growth
  stays gVisor's.
- The NAT: `InitialWindowSize` the bet, `MaxWindowSize` the share, the
  ladder unchanged; a download send bound of `min(advertised, share)`,
  which is new.
- H3: `H3InitialStreamReceiveWindowByteCount` the bet,
  `H3MaxStreamReceiveWindowByteCount` `min(T × rtt_assumed_max / f,
  share of PlatformTransportBudget)`, the connection window keeping
  its ratio; quic-go's growth stays. The server mirrors it in its tree,
  and until it does the server's constant is the binder on two
  hop-directions, which the namespace cell will show.
- The carriers' and upstream sockets: the `DialControl` request
  becomes `min(T × rtt_assumed_max, share)`, on the carrier dialer as
  well as the upstream one; §15's rule decides whether that pins or
  leaves autotuning alone, as it does today.
- Unchanged: the item buffers, the UDP socket buffers, the coalescer.

### 37.10 Implementation order, each step measurable alone

1. The Transfer send rule's interval and form (§36.7): minimum round
   trip, rate between samples. Measured at 200 ms: computed window 16
   → about 9.3 MiB, throughput unchanged at 164, startup excess about
   halved.
2. The receive advertisement (§37.3) and the hold as a share. Measured
   in a loss cell at 200 ms with a 16 MiB send window: retransmitted
   bytes per loss event fall from about a window to about an item, and
   `ReceiveQueueDropCount` goes to zero.
3. The surface (§37.4) on Transfer first: target, assumed round trip,
   shares. Measured: the shipping 2 MiB arm reproduced by T × assumed
   round trip equal to 2 MiB, so the before and after are one binary
   with the assumption written down; then the startup excess against
   the bet.
4. The tun's ceilings and initial from the surface. Measured: the
   4 MiB plateau moves with the share, halved and doubled at 200 ms,
   §36.4's discriminator run in the other direction.
5. The NAT's initial, share and download bound. Measured in upload and
   download cells at 200 ms; the download bound measured as provider
   memory at forty clients, which today is the clients' choice.
6. H3 ceilings and initial from the surface, with the server's mirror.
   Measured in the namespace cell: the 109 Mb/s plateau of §36.2 moves
   with the share; until the server mirrors, it does not.
7. The dialer requests from the surface. Measured in the namespace cell
   on H1 and against a delayed origin leg.
8. The copy elimination of §37.7, after the above, as its own
   measurement of provider memory at scale.

Each step is one knob at a fixed delay, and its prediction is written
above before it runs.

### 37.11 At the target: one gigabit per second, worked through

The user has set the target at one gigabit per second for every layer.
This section designs against 1 Gb/s = 125 MB/s, decimal; if it was
meant as one gibibyte per second every figure below multiplies by 8.6
and the budget binds that much sooner. The constant is one setting,
`TargetThroughputBytesPerSecond`, and nothing below depends on its
value except through it.

The window one layer needs at the target, goodput, with the framed
layers' figure at 0.865 in brackets:

    round trip    window            framed
    1 ms          125 kB            145 kB
    10 ms         1.25 MB           1.45 MB
    25 ms         3.1 MB            3.6 MB
    50 ms         6.25 MB           7.2 MB
    100 ms        12.5 MB           14.5 MB
    200 ms        25 MB             28.9 MB

At the target the k × achieved term of §37.4's rule is not the
binder, since achieved equals T, so the window is T × rtt exactly and
the memory per copy is that row.

How the budget is divided across the layers. Dividing it evenly is
wrong, and my first draft of this section was wrong in the opposite
direction for an instructive reason: I argued the copies hold the same
bytes and so need the same room. They hold the same bytes for different
lengths of time. The carrier holds a byte until its hop acknowledges it,
one hop's round trip; Transfer holds it until the far sequence
acknowledges it, both hops plus our delays; the NAT or the tun holds it
until the inner acknowledgement returns, the whole tunnel both ways. At
one rate the three holds are T × rtt_D, T × rtt_C and T × rtt_B, and
in production those stand roughly as 1 : 2 : 4, with the innermost
layer the largest. In the cell, one wire, they are equal, which is why
the cell could not have shown this.

So the division rule is the one that makes the arithmetic come out and
needs no knowledge of the binding order: each layer's share is its own
need, `T × rtt_L` from its own estimator, and when the sum of needs
exceeds M every need is scaled by the same factor, `M / Σ need`. Under
that rule every layer's window over its own round trip is the same
number, `T × min(1, M / Σ need)`, so no layer binds ahead of another:
the chain is co-binding, delivers `min(T, M / Σ rtt_L)`, and no other
division of the same bytes delivers more, because any other division
lowers the smallest window-over-round-trip. That answers the question
directly. A layer need not know it is the binder, and the order need
not be assumed at design time; each layer knows its own round trip,
which is the only thing the rule asks of it, and the arbiter applies
one factor to all. A layer can be seen to be the binder at runtime,
its window fully in use while the others show slack, and the estimates
expose that for the record, but the rule does not depend on it. A
static division with an assumed order fails exactly when the ratio of
the loops' round trips changes, which it does between the cell (1 : 1
: 1), production (1 : 2 : 4) and a distant origin (loop A dominant).

The mechanisms exist for the layers whose growth we do not own. gVisor
reads its buffer limits from the stack option on every autotune step
(`GetTCPSendBufferLimits(e.stack)` in `computeTCPSendBufferSize`), so
re-setting `TCPSendBufferSizeRangeOption` moves a live endpoint's
ceiling. quic-go's connection flow controller takes an
`allowWindowIncrease` callback (`flow_controller_base.go:70`,
`Config.AllowConnectionWindowIncrease`), a runtime veto on growth that
is a budget hook in all but name; the static maxima are set from the
budget's upper bound and the callback enforces the live share. The
kernel's socket buffers can be re-set with `SO_SNDBUF` and `SO_RCVBUF`
at any time, at the cost §15 documents of locking autotuning, so for
loop A the share is applied through the pin request and otherwise left
to the kernel. Transfer's shares are ours directly.

The phone under that rule. A 24 MiB budget is 25.2 MB. The achieved
rate on a long path is `M / Σ rtt_L` over the copies, so in the cell's
regime, where the three round trips are equal, the target holds to a
67 ms wire and falls to 670 Mb/s at 100 ms and 335 at 200; in the
production ratio the sum is 1.75 × the tunnel round trip, the target
holds to a 115 ms tunnel and falls to 576 Mb/s at 200 ms. The layers
that get less than they asked for lose nothing in consistency, since
all sit at the same window over round trip; what falls is the rate,
visibly, which is the intended failure mode. Two levers move the knee:
the copy count, which §37.7's buffer sharing takes from three to two on
the send side, and our own acknowledgement delay, which is inside every
one of the three round trips and is the only term that is ours to
shorten at every rate.

The provider. Forty clients at 100 ms want 12.5 MB each for one copy
and about 22 MB each across the copies in the production ratio, close
to a gigabyte, and a provider is unbudgeted today, which is why the
arithmetic never surfaced. Two systems are possible, they have
different failure modes, and this design chooses the first:

- A divided budget, chosen. The provider gets M from its role profile.
  Each client's share is its need under the same rule as the layers,
  scaled by one factor when the sum of clients' needs exceeds M, so a
  client's window shrinks as others arrive and every client's rate
  falls together, `min(T, M / Σ over clients of Σ rtt_L)`. The target
  is an intent, a ceiling per client, and the provider's uplink caps
  the aggregate rate long before forty gigabits; the budget arbitrates
  memory and the uplink arbitrates rate, and both shortfalls are
  visible in each client's estimate. Its failure mode is graceful
  degradation, which is what TCP does across flows on a link and what
  a provider's users already experience from its uplink. The
  floor-and-borrow admission the queues already have (§29) is this
  rule's implementation: the floor is the least a client is lent, the
  borrow is the scaled need.
- Admission, not chosen. The target as a promise: the provider serves
  at most `M / (Σ rtt_L × T)` clients at their full windows and refuses
  the rest. Its failure mode is refusal, it needs the contract layer to
  carry the refusal, and it would admit about one client per gigabit
  of uplink regardless of memory. It is named so it is not chosen by
  accident. What the divided budget keeps of it is the floor: when
  `N × floor` reaches M the provider is at capacity. The NAT's
  `GlobalLimit` is already derived from a memory target through an
  assumed per-flow byte count (`ip.go:595–596`,
  `natTarget × 2/5 / providerTcpFlowByteCount`); that byte count is the
  same hidden constant in another place, and the floor replaces it, so
  the one admission bound this landing touches is derived rather than
  invented.

The short path, where the scheme pays for itself with no tension. At
1 ms and 10 ms the target wants 145 kB and 1.45 MB framed, both below
the 2 MiB allocated unconditionally today, so on short paths the new
scheme hits the target with less memory than the constant it replaces.
The crossover is at 14.5 ms of loop-C round trip, where 2 MiB framed is
exactly the target's window. That number is worth having beside §36.3:
the path's own acknowledgement delay, δ plus the writer's service, is
about that size, so on a path whose network round trip is negligible
the target wants about 3.6 MB framed, more than today's constant, and
every millisecond of our own delay removed is 125 kB per layer per
client at the target. The change is faster on long paths and cheaper on
short ones, and the boundary between the two is a number, not a
judgement.

The initial size at the target is a guess at the path: 145 kB bets on
a local one, 3.6 MB on a wide-area one, 29 MB on a 200 ms one. The
trade is quantifiable. The measured startup excess was 100 ms from a
2 MiB bet, which at the target is a 14 ms bet, to the 4.6 MiB framed
the cell could use at 200 ms: 1.2 doublings, about 0.4 round trips per
doubling with the sum-form estimator, and the rate form of §36.6
should halve that. So a 145 kB bet on a 200 ms path is 7.6 doublings,
about 0.6 s of excess per sequence start; a 3.6 MB bet is 3 doublings,
about 0.25 s; a 29 MB bet has no ramp. In the other direction, a 3.6 MB
bet on a 1 ms path with a slower layer below it stands as queue until
the estimate is trusted, `SampleCount` samples at that round trip, which
is milliseconds, and the tun's own 1 MiB default bounds what can arrive
in the meantime; a 29 MB bet on the same path is the same brief queue
at eight times the size. Both errors scale with the round trip they are
wrong about, but the low bet's cost is paid in whole on every long
path and the high bet's is paid briefly on every short one. Per role,
then, from the argument: a phone bets low, because its sequences start
often and its interactive flows share the writer with the bulk ones; a
provider bets at the wide-area row, because its client sequences are
long-lived and the harm of the bet on a short path is a queue that
lasts one estimate. The assumed round trip per role is the setting, it
is written beside the target, and the campaign picks it.

### 37.12 Three findings from the implementation stream, and the one policy they share

The implementation stream measured the interval defect of §36.5 on our
cell: `ScaledRtt` reads 300 ms at every delay, against measured round
trips of 6.7, 27 and 102 ms, so the rule accumulated between 2.9 and
44.8 times the bandwidth-delay product, overshot to its ceiling, and
the ceiling was the operative bound. The 1.7 measured in §36 was a
large fixed window against a small one, with a ramp; the mechanism,
sizing from the path, was not demonstrated by that cell. That is
recorded as what it is. The three findings and the decisions:

The interval. The proposed one-line fix substitutes the sampled mean
for `ScaledRtt`. Two things stand between that and the design, and
both bite hardest at short round trips, which is where the defect was
worst. The mean contains the queue the window creates (§36.5,
tag stamped ahead of the writer), so on a path bound below the sequence
it feeds back; the multiplier is `RttEstimate.Min`. And the sum over a
horizon cannot read a short interval at all: the ring advances a sample
every 75 ms (`transfer.go:8774`), so `deliveredBytesOver(6.7 ms)`
returns whatever was delivered since the newest sample older than
6.7 ms, between 7 and 82 ms of delivery, which is 1 to 12 times the
bandwidth-delay product chosen by where the ring happened to be. The
form is a rate, bytes between two samples over their spacing, times the
minimum round trip (§36.6, §36.7), with the ring's cadence reduced to
match. Two predictions, stated before either change is made. At 200 ms
the corrected rule converges to twice the path's delivery per round
trip, about 9 MiB framed, and its transfer-average throughput sits
within the null band of the 8 MiB constant arm's 129.5 Mb/s, above the
overshooting rule's 117.6, because the gain is one fewer doubling in
the ramp and the steady state was already equal (§36.6); this holds for
the mean form as well as the rate form. At a 5 ms delay the two forms
separate: the sum-form window scatters between 2 and 24 times the
bandwidth-delay product from one estimate to the next, visible in the
estimate's `Window` field, and the rate form holds at 2. If the rate
form scatters too, the ring's cadence was not the cause and the
estimator has a defect this section did not find.

The receiver. The `Ack` carries nothing about remaining capacity
(§37.3), and the coordinator asks whether sizing the receive hold from
the path is sufficient or the layer needs flow control it has never
had. It needs the flow control, and the argument is not the size of
the mismatch but who can know it. A receiver has no round trip to size
from; it could mirror a sender's window only if the sender told it,
and the sender's ceiling is a deployment setting the receiver cannot
see, unscaled where the hold is memory-scaled, so on a small host the
two diverge further with every budget step. The only party that knows
what a receiver can hold is the receiver, and the only mechanism that
makes a sender respect it by construction is an advertisement, which is
what every other reliable layer on this path already has. The wire
change is one optional field, `receive_window_byte_count` on the
`Ack`, computed as the hold's share less what it holds, backward
compatible because absent means legacy. It is a smaller piece of work
than it looks, and the alternative is not smaller: sizing the hold to
"whatever a sender might send" is the sender's ceiling, which a 32 MiB
host cannot hold and a receiver cannot learn. Sizing the hold from the
path is not sufficient because there is no path at the receiver to
size from.

The budget. It is optional and defaults to nil, and without one the
ceiling is a fixed per-sequence maximum, so the property that protects
a provider holds only where a deployment attaches a budget. The
implementer's suggestion is that the rule decline to size above its
floor when no budget is attached. Adopted, and generalised, because it
is the same shape as the other two:

    the sender sizes only against a bound it can see, and holds the
    floor where it cannot: a budget for memory, an advertisement for
    the receiver, an estimate with samples for the round trip.

Absent a budget, the window is the floor. Absent an advertisement, a
legacy peer, the ceiling is the receive hold's shipping constant, 2.5
MiB unscaled and less at a budget, which is the most a legacy receiver
is known to hold; the rule is then inert against old peers and safe
against them, and it engages fully only between peers that both carry
the field. Absent samples, the window is the initial size of §37.5.
One policy makes the safe configuration the default and the unsafe one
impossible rather than discouraged, which is the property a comment
cannot provide.

### 37.13 Admission and occupancy: what the no-delay guard measured, and the slow last mile it did not

The coordinator predicted the no-delay guard would fail on memory,
because the interval defect makes a short path the worst case: at a
5 ms delay the rule used 300 ms as its interval and computed a 16 MiB
window against a 2 MiB fixed arm. The windows confirmed that. The
memory did not follow: peak pool 1.043 against 1.051 MiB, the larger
window higher in five of ten runs, peak heap within 80 KiB against a
null band of 656, both deltas negative at 64 MiB. Peak pool was 0.066
of the 16 MiB window and 0.52 of the 2 MiB one, which is the same
1.04 MiB in both arms, and that number is the loop's bandwidth-delay
product at the cell's rate: the path put the same bytes in flight
whichever window permitted more. A window is an admission limit;
occupancy is what the path fills. §37.7 is restated in those terms.

Do they need different mechanisms. Yes, and Transfer already has both,
which is the useful finding: the per-sequence window bounds admission,
`CanAdd` against the estimate's `Window`, and the shared pool bounds
occupancy, because `ResendQueueBudget` is charged by bytes actually
queued and its floor-and-borrow admission refuses when the pool is
full (§29). The companion policy the coordinator names, a sender stops
admitting when what it holds approaches its share regardless of what
its window permits, is the pool's admission when a budget is attached,
and §37.12's rule that no budget means the floor is what makes it
always present. So the budget divides occupancy and not permission:
permissions may sum past M across clients, since on every path shorter
than the knee they are not held, and the pool's floor guarantees each
client its least and its borrow hands the rest to whoever fills it.
For the layers whose growth we do not own, occupancy cannot exceed
permission, a full buffer is the window, so their permission ceilings
from the surface bound occupancy conservatively, and quic-go's growth
veto can be driven by pool occupancy rather than by a constant. The
failure mode moves accordingly: not a window shrunk in advance for a
path that might have needed it, but admission refused at the pool
when the held bytes reach it, counted and visible.

The advertisement does two jobs. A receiver that advertises remaining
capacity, its share less what it holds (§37.3), is telling the sender
about occupancy, not permission, and that is the quantity that turned
out to matter. Its first job was loss recovery: a sender's window may
never exceed what the receiver can buffer around a hole. Its second is
memory: a receiver at its budget throttles its senders instead of
dropping, which is what TCP's window has always been, an occupancy
signal from the receiver's buffer. The pool is the occupancy mechanism
for the send side and the advertisement is the one for the receive
side, and between them occupancy is bounded at both ends without
dividing permission at either.

The slow last mile, which the guard did not clear. Its cell has no
bottleneck below the sender's rate, so the queue drains as fast as it
fills. On a path bound below the sender, at a rate r, the sender fills
its window and the excess stands as queue: occupancy is r × rtt plus
the standing queue, the standing queue is the window less r × rtt, and
every flow of that client sharing the writer waits behind it for
window over r. Predictions for the cell the harness is building, the
added delay per arm, which is rate-independent where it is the rule's
own doing. Falsified for TCP, every row, by the cell (§37.14): a TCP
sender is only handed what the inner protocol's flow control offers.
They stand, unchanged, for UDP, which has no such control, and that is
where they are now to be tested:

- the shipping constant, 2 MiB: 2 MiB / r, 840 ms at 20 Mb/s, 170 at
  100;
- a 16 MiB constant: 16 MiB / r, 6.7 s at 20 Mb/s;
- the rule as built: twice r × 300 ms over r is 600 ms at any rate
  above the floor's, and the floor's 2 MiB / r below it, so never less
  than 600 ms; the interval defect is a latency defect on slow paths,
  not only a sizing one;
- the rule with the minimum round trip and the rate form, and a floor
  of a few packets: (k − 1) × rtt_min, one propagation round trip at
  k = 2, tens of milliseconds.

The reading that shows it is `Rtt.Mean − Rtt.Min` on the sequence,
which is the standing queue in time, beside the pool's occupancy.

That last row exposes a defect in the formula as I first wrote it, now
corrected in §37.4 and §37.9. The initial size was the lower clamp of
the rule, so a provider's wide-area bet of 3.6 MB would have stood as
1.4 s of queue on a 20 Mb/s last mile for as long as the sequence
lived. The initial is the value before the estimate has samples and
nothing after; once sampled the rule may shrink to k × r × rtt_min,
which on a slow path is far below today's constant, and the only floor
under it is a working minimum of a few packets, which
`ResendQueueMinByteCount` already is for reliable admission. A bet
that cannot be walked back is not a bet.

### 37.14 The slow drain: TCP bounds itself, and UDP is the route left to both costs

Sixty of sixty runs valid. At a 20 Mb/s drain every arm added between
2.3 and 2.9 ms of delay against §37.13's predictions of 840 ms, 6.7 s
and never below 600 ms; peak send queue was 19 to 21 KiB, a hundredth
of the window or less, and identical whether the window was 2, 3.6 or
16 MiB; occupancy was smaller under the slow drain than under an
unlimited one. All four rows were wrong for one reason: I had a sender
fill its window, and a TCP sender is only handed what the inner
protocol offers. The slow carrier is seen by the client tunnel's
receive side, whose moderation sizes the advertised window from bytes
copied per round trip (§36.4), so the window closes to the drain's own
bandwidth-delay product, the origin's TCP backs off through the
provider's socket, and the transfer layer is handed 20 KiB, which is
the drain rate times the carrier's round trip and nothing more. The
window cannot cost memory it is never given, and the floored interval
cannot cost latency through a queue that never forms. The same holds at
full speed, where the queue sits near half a mebibyte regardless of
window.

So the composite memory bound of §37.7, in its occupancy form, is
correct and largely inoperative for TCP: the quantity it bounds is set
by the inner protocol's flow control and not by anything this design
configures. The budget is not what keeps a TCP flow's memory bounded;
TCP is. The budget's job is the cases where that protection does not
exist. One direction is still owed a reading: the mechanism above is
the receive side's, on download. On upload the inner flow control is
the NAT's advertised window, which sizes from loop A's backpressure and
not from the carrier (§37.2, B4), beside a congestion window that grows
without loss on a reliable carrier; either something I have not found
bounds it, or upload at a slow drain fills the client's transfer queue
to the smaller of its window and the tun's 4 MiB send buffer. The
cell's record says which direction it ran, and if it ran only download,
upload is the second cell.

UDP has no end-to-end flow control, and the program's own cells show it
does not share TCP's ceiling, 1.39 Gb/s against 0.3, so a UDP source is
exactly what can outrun a slow drain. What bounds it today, from source,
on the return path from origin to client:

- `UdpSequence.receivePacket` hands each datagram to the return path in
  `receiveRecoveryModeNonblocking` (`ip.go:3189–3200`);
- `enqueueReturnItem` offers it to a per-shard channel with a
  non-blocking send and drops on a full channel, counted in
  `congestionDrops.addReturnQueue`; the channels hold
  `ReturnSendQueueSize` = `MemoryScaledCount(256, 64)` items in
  aggregate (`ip.go:6387,7676–7700`);
- the return sender calls `SendWithTimeout` with `returnWriteTimeout`,
  which is `WriteTimeout` for a TCP socket item and zero for everything
  else (`ip.go:8038–8043`); at zero, `SendSequence.Pack` refuses rather
  than waits, at the slot admission and again at the 32-item `packs`
  channel (`transfer.go:6105,6215–6250`), and the channel is what fills
  when the sequence goroutine stops draining it because the byte window
  is full (§32.1);
- `retryReturnSend` retries only TCP socket items; for anything else a
  refused send returns false on the first attempt and the datagram is
  dropped, counted in `congestionDrops.addReturnSend`
  (`ip.go:7983–8026,8137`).

So a UDP return flood fills the transfer send queue to its byte window,
plus 32 items, plus the return shards, and everything past that is
dropped at once: no blocking, no retry, no backpressure into the kernel
socket, whose own drops (`udpKernelReceiveDropCount`) occur only when
our reader is slower than arrival, not when the carrier is. Memory is
bounded by the window. Latency is the window: every admitted datagram
waits window over drain rate, 840 ms at 2 MiB and 20 Mb/s, 6.7 s at
16 MiB, never less than 600 ms under the rule as built, and so does
every TCP packet of the same client, because UDP and TCP returns share
the per-destination sequence in first-in first-out order, lanes being
shipped at zero. The four rows of §37.13 are UDP's rows. And the
head-of-line cost is §35's mechanism with a UDP source in place of the
zombies: a concurrent TCP flow's round trip rises by the standing
queue and its throughput falls to its window over that round trip.

On the client side the tun's link endpoint waits at most
`OutboundQueueWaitTimeout`, 250 ms, for outbound queue space and drops
the rest of a write (`tun.go:65,127–132`); the device layer's timeout
into `SendPacket` is chosen in the sdk tree, outside this one, and the
in-tree delegations pass zero (`ip.go:9177,9236`), which refuses and
drops. A refusal is an inner loss: for TCP from the application it
halves the congestion window, which is self-limiting; for UDP it is a
loss to the application, which is UDP's own semantics.

Whether the advertisement reaches UDP: it does, because UDP returns
ride the same sequence as TCP returns, the provider's reply key echoes
the client's key at lane zero (§27.5, §30.2), and nothing in this tree
requests the protocol's no-acknowledgement Pack for IP traffic (the
`noAckSendRecord` path has no caller). So the `Ack`'s
`receive_window_byte_count` bounds the sender's window for the whole
sequence, UDP included. What it bounds is the receiver's memory: a
receiver at its share throttles the sender instead of dropping. It
does not bound latency; a 16 MiB advertised hold still lets a sender
stand 16 MiB of UDP in front of a 20 Mb/s drain. Latency for UDP is
bounded by exactly one thing in the design, the k × achieved term of
the window rule, which measures the drain from acknowledgements and
holds the window at k times the drain's bandwidth-delay product, so
the standing queue is (k − 1) round trips and the excess is dropped at
admission, early, which for UDP is correct. With the shipping constant
that term does not exist and the delay is 2 MiB over the drain; with
the rule as built it is never less than 600 ms; with the interval and
the floor corrected it is one propagation round trip.

So the statement the coordinator asked for, plainly: the budget, the
advertisement and the window rule's delivery term are load-bearing for
UDP specifically, and inert for TCP on a slow drain, where the inner
protocol supplies all three protections itself. The design should not
imply uniform protection; it should say that UDP is the traffic the
Transfer layer's own flow control exists for, and that a slow path
with a UDP source is the regime in which every constant this program
has enumerated turns from a throughput ceiling into a latency floor.
There is one more window in that regime the enumeration missed: the
unreliable carrier's flight controller, slow start and additive
increase on acknowledgement, halved on loss, between
`UnreliableInitialFlightByteCount` 8 KiB and
`UnreliableMaximumFlightByteCount` 256 KiB (`transfer.go:734–736`,
`transfer_flight.go:222–320`). Path-sized growth under a constant
ceiling, the same defect shape, and the tightest ceiling in the chain:
256 KiB encodes 84 Mb/s at 25 ms and 10 at 200. It is D6 in §37.2's
list and takes its ceiling from the surface like the others.

Two corrections to carry. The slow-drain cell cannot confirm the
initial-size clamping defect of §37.13, because its old-clamping arm
showed no added delay either: nothing filled the queue, so nothing
distinguishes a rule that can shrink from one that cannot. UDP is the
discriminator for that defect as well as for the interval's latency
cost, two open questions with one cell. And three instrument faults
were found and fixed on the way, two of which would have inverted the
result: a pacer that paced to timer granularity rather than rate, a
carrier route whose own thousand-frame buffer absorbed the backpressure,
and occupancy inferred from a global pool that charged unrelated
buffers. The second is the item-bound wire §36.4 named as the one
place an in-process carrier could charge framed bytes; it existed. All
three are the shape this program has now seen seven times, something
accurate standing in for the record that decides, and the fixture's
wire capacity now belongs in every cell's record as a field.

Predictions for the UDP cell, stated before it runs, at a 20 Mb/s
drain with a UDP source above it: added delay 840 ms for the shipping
constant, 6.7 s for a 16 MiB constant, not less than 600 ms for the
rule as built, and tens of milliseconds for the corrected rule; a
concurrent TCP flow's `Rtt.Mean − Rtt.Min` equal to that delay in each
arm; the old-clamping arm with a 3.6 MB bet at 1.4 s against the
corrected rule's tens of milliseconds; `congestionDrops` counting the
excess in every arm rather than the kernel's drop counter; and
occupancy at the window in every arm, which is where the budget, with
forty such clients, is the only thing between a provider and forty
times its ceiling.

### 37.15 The UDP cell: the excess is loss, the class of error, and what of the fix is measured

Forty-eight of forty-eight valid, 97 Mb/s offered against a 20 Mb/s
drain. Every arm added 1.6 to 1.8 ms against §37.14's 840 ms, 6.7 s
and never below 600; peak queue 16 to 20 KiB whether the window was 2,
3.6 or 16 MiB; 86.7 per cent loss in every arm; every drop at the
return send, the downstream sequence refusing admission, none at the
ingress handoff or the return queue. The source reading of §37.14 was
exact and the inference from it was wrong: a non-blocking admit with a
zero timeout converts excess into loss, not into delay and not into
occupancy. I wrote the mechanism that cannot build a queue and then
predicted the queue.

The class, named so it is watched for. Twice now the source reading
was accurate and the step from mechanism to consequence added a queue
the mechanism excludes: on TCP a sender that fills its window, when
the inner protocol hands it only what it offers; on UDP an excess that
waits, when the admit refuses. The rule for every prediction of delay
or occupancy from here: name the buffer the bytes would wait in, its
bound, and the code that fills it, and check that the code has
somewhere for them to wait. A non-blocking admit is loss. A blocking
write is the layer below's acceptance, not our window. And the fact I
had in §32.1 and did not apply either time: the sequence goroutine
writes a Pack to the carrier before it enters the resend queue, so the
resend queue holds only what the carrier has accepted. Against a
carrier that accepts at its drain rate, the queue is the carrier's
in-flight, 20 KiB at 20 Mb/s over the carrier's round trip, and no
window above that is ever reached.

Where occupancy could approach the window, from that fact. It needs a
layer below the sequence that accepts faster than the far end drains,
or a source of bytes that is not backed off:

- A fast first hop into a buffer with a slow hop beyond it. The
  platform relay is the one such layer on the production path, and its
  queue bound is in the server tree, unread here; the harness's
  thousand-frame route buffer was a model of it, and it absorbed the
  backpressure exactly as such a buffer would. A carrier socket pinned
  large would be another, and none is: §15 pins only the upstream
  socket. quic-go accepts only what its congestion window allows, and
  an autotuned kernel socket about twice its own bandwidth-delay
  product, so neither carrier in this tree hands the sequence a queue
  beyond one or two round trips of its own hop.
- A fast wire with a long round trip. The queue fills to the window,
  in flight, which is the throughput case and not a harm: the 200 ms
  cell held 4.6 MiB with 16 MiB permitted, bounded by the inner window
  for TCP and by the source rate for UDP.
- The receive side under reordering. A single reliable carrier delivers
  in order and loses a contiguous tail on failure, which the sender
  resends in order, so the out-of-order hold is never used; with more
  than one route in the transport window, frames are striped across
  routes and arrive out of order routinely, and when one route dies
  its frames are a scattered subset, so the hold fills with the other
  routes' arrivals up to 2.5 MiB and evicts or refuses beyond it, all
  of which is retransmitted. That is reachable, in production
  configuration, and constructible: two routes at 200 ms with 16 MiB
  permitted and one route killed mid-transfer.
- Upload. Untestable in this cell, whose source calls the provider's
  receive path directly with no client send buffer, sequence or resend
  queue, so every result in this sequence is download only for both
  protocols. On upload the bytes that wait are in the tun's send
  buffer, which autotunes to twice a congestion window that never
  sees loss on a reliable carrier and so reaches its 4 MiB maximum per
  connection with the application's write blocked behind it. That is
  TCP's own socket buffer doing what a socket buffer does, its memory
  is the tun's constant and not the transfer window, and the shared
  queue on that side is the tun's outbound queue, bounded by
  `OutboundQueueWaitTimeout`. A client-side cell is needed to measure
  any of it.

None of these is a configuration in which the transfer window's
permission becomes occupancy through the carriers in this tree with a
single route. The memory argument for the transfer window rests on the
relay and on multi-route reordering, and both are unmeasured.

What the delivery term is for, in those words. Measured: nothing beyond
a larger permission; the 8 MiB constant did as well as the sized arm
at 200 ms, better in fact, and the 16 MiB constant cost nothing in any
cell. The harm I argued it prevents, a UDP source standing a queue in
front of a TCP flow on the shared sequence, is unreachable through
this tree's carriers because the excess is dropped at admission and
the carrier bounds its own acceptance; it is reachable only through a
deep buffer below the sequence, which is the relay case above. The
latency cost of the interval defect, 600 ms by my arithmetic, was
measured at 2 ms for the same reason. So the term's justification is
structural: it is what bounds the standing queue where a buffer below
the sequence would let one form, and it is cheap. It is not what the
throughput result needed.

What of the composite fix is measured and what is structural, so that
the landing is sized to the evidence:

- Measured: the 2 MiB transfer window binds throughput at long round
  trip, and a larger permission lifts it to the next binder at no cost
  in memory or latency in any cell; the next binder is the tun's 4 MiB
  send buffer, measured; the H3 stream window at 3 MiB is the next on
  a real carrier, predicted for the namespace cell. The fix that this
  evidence supports is raising those three ceilings consistently, with
  the surface of §37.4 as the form that writes the assumption down.
- Structural, cheap, and worth landing on the argument: the interval
  and floor corrections of §36.7, because a rule that multiplies by
  300 ms is wrong whether or not a cell can show it; a mandatory
  budget as a cap on permission, §37.12.
- Structural and to be measured before it lands: the receive
  advertisement, whose one reachable harm is the multi-route failover
  above. Prediction for that cell: with 16 MiB permitted and 4.6 MiB in
  flight, killing one of two routes evicts or refuses about 2 MiB at
  the hold and retransmits it, `ReceiveQueueDropCount` above zero and a
  throughput dip of several round trips; with the advertisement, zero
  evictions and a dip of one; with the shipping 2 MiB window, zero
  either way, because 2 MiB is under the 2.5 MiB hold, which is the
  accident of ordering that has protected it so far.
- Deferred until a regime needs them: the proportional division of
  occupancy across layers and clients of §37.11, and the occupancy
  pooling of §37.13. They solve a compounding that no cell has
  produced, and the record should not carry them as if it had.

The implementation order of §37.10 stands with its first three steps
reordered by that: the ceilings first, then the interval and the
budget, then the advertisement behind its cell, and the rest behind a
regime. The prediction that remains open and is the program's, since
it is the one the user's framing turns on: on a real carrier at 200 ms
the sized arm's multiple over the shipping constant holds until the H3
window binds at 109 Mb/s, and raising that window moves it.
