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
