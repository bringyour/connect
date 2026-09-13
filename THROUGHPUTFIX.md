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
