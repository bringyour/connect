# Provider throughput: peer review, corrections, and a measured fix

A review of the provider speed fixes reported on `beta/custom-server`, the
defects that review found in them, the additional fixes it produced, and one
new mechanism that is now measured.

Written for the reporter as much as for the record. It stands alone from
`THROUGHPUTFIX.md`, which carries the numbered design sections, and from
`tests/PERFVAR-MEASUREMENTS.md`, which carries the campaign ledger.

Status: current as of 2026-09-14. Four measurements are still running and are
marked OPEN where they appear.

---

## 1. Verdict on the two submitted fixes

Both mechanisms are real. Both were confirmed in source before any measurement
was taken. Both are on `main` as the baseline for this work. Neither is left
as submitted, and one of them currently carries a regression.

### 1.1 The upstream receive buffer

**The mechanism is correct.** An explicit `SO_RCVBUF` on a connected socket
sets the buffer lock. Autotuning is then the only thing that could raise the
window, and it is disabled. The flow keeps the clamped window for life.

**The fix as submitted is unconditional, and that is wrong.** Whether deleting
the pin helps or hurts depends on whether twice the requested size beats the
kernel's autotuning ceiling, which is a property of the host. Measured across
provider memory budgets, on a download:

| Budget | Deletion applied (main) | Pin restored | Effect of the deletion |
|---|---|---|---|
| 1 MiB | 143.0 Mb/s | 44.4 | 3.2x faster |
| default | 270.6 Mb/s | 437.2 | **38% slower** |

The default budget is what a provider runs. So the unconditional deletion is a
regression in the configuration that matters most, on our runner.

**Both results can be true at once.** The comparison that decides it is two
integers on the host: `net.core.rmem_max` against `net.ipv4.tcp_rmem[2]`, and
the same pair on the write side. The reporter confirmed the mechanism
independently by raising `tcp_rmem`, which is consistent with this reading.

**What replaces it:** a runtime rule that pins a direction only where the
pinned buffer would beat the kernel's autotuning ceiling, computed differently
on Linux and Darwin, never on an unknown platform. Receive pins only before
connect, through a new dial hook. Against main it ties at three budgets and
wins 363-403% at the smallest.

### 1.2 The send buffer, which the report does not mention

`SetWriteBuffer` is called on the same already-connected socket. The severity
is asymmetric: receive is a freeze, send is a ceiling on unacknowledged bytes.

Measured, and the arithmetic is exact rather than plausible: at the 1 MiB
budget an upload reads 83.8 Mb/s, and 512 KiB of pinned send buffer over the
50 ms path predicts 84.

Deleting this pin is worth **18.4% at the shipping ack compression and 25.9%
at 12 ms**, twelve of twelve paired repetitions, disjoint distributions. The
gap widening at the faster setting is the prediction confirmed: a buffer binds
only when it sits below the path's next limiter.

### 1.3 The dead-client release

**The mechanism is correct.** `retryReturnSend` loops until the source's
lifecycle context ends; only a Reliability verdict creates that terminal
state; the platform issues one only for a contract request to an inactive
destination, which a wedged flow never makes.

**The reported reason it never recovers is not the reason.** The platform's
Reliability verdict reads `network_client.active`, an identity lifecycle. A
contract probe could not detect a dropped client even if the flow made one.
That rules out an entire class of simpler fix, so it is worth stating
precisely.

**The signal as submitted produces false positives.** It decides on one item's
admission, which is a fact about the provider's own queue rather than about
the client. A live client acknowledging slowly with flows parked loses the
broadcast race for every freed slot and is retired although it is alive.

Deterministic tests that fail on the submitted tree:

- `TestLiveClientStalledPastAbandonTimeoutIsNotRetired`
- `TestSlowLiveClientWithManyFlowsIsNotRetired`
- `TestReleaseTracksClientDeathNotItemProgress`
- `TestFirstAdmissionAfterAParkedStartRestartsTheClock`
- `TestSilenceIsInadmissibleWithoutACarrier`

**What replaces it:** per-source acknowledgement evidence (outstanding count,
last ack, stall start, carrier absence) as the send-ack target of every
socket-owned return, with silence inadmissible when no transport can reach the
client. One honest trade survives and is documented: a client that goes fully
silent past the timeout and then resumes is still released, on both trees.

### 1.4 A leak in the submitted helper

`startUnreachableProviderReturn` calls a borrowing entry point with a copied
packet and never returns it. One 256-byte pooled root per call.

Found by running, not by reading, under a new ownership assertion. It is in
code cherry-picked onto `main`, so it is ours now regardless of origin. Fixed.

Small and unbounded is how a leak starts.

---

## 2. Corrections to the report's framing

### 2.1 The ceiling is not aggregate. One flow reaches it.

The report presents ~640 Mb/s as an aggregate ceiling reached at eight flows.
Recomputing the published tables by cell, with fix 1 applied:

| Path | Median |
|---|---|
| One flow, CDN | 664.5 Mb/s |
| Eight flows, CDN | 677.0 Mb/s |
| One flow, synthetic in-memory server | 671.0 Mb/s |

Eight flows are 2% faster than one. Three structurally different
configurations land in the same band.

This rules out every per-flow cost and points at a single shared resource one
flow can saturate. It also explains the report's own observation that no
component is CPU-saturated: a serialized handoff binds at low utilisation.

(Four repetitions per cell, so the 2% is noise. The claim is the *absence* of
any eightfold effect, which is far outside it.)

### 2.2 The synthetic control is stronger than presented

It was offered as a negative control for fix 1, which it is. It also shows the
ceiling survives removing the provider's upstream kernel socket from the path,
which narrows the candidates to what all three configurations share.

### 2.3 The 8 Mb/s per zombie is a transient, not a steady state

The report gives ~8 Mb/s of retransmission per dead client. The resend
interval sequence, read directly over 12 seconds, is two doublings then pinned
at the ceiling. A zombie reaches the 8-second `MaxResendInterval` about six
seconds after the kill.

| Reading | Per zombie | Forty zombies |
|---|---|---|
| Reported figure | ~8 Mb/s | 336 Mb/s |
| Actual steady state | 2.1 Mb/s | 84 Mb/s |

So against the reported 460 Mb/s loss, the unexplained remainder **grows** from
~124 to ~376 Mb/s. Bandwidth carries under a fifth of the coupling. The open
question in the report is harder than it looked, not easier.

### 2.4 Two claims we could not reproduce

The window does not freeze at 64 KB on the kernels we tested; the clamp
follows the pinned buffer. And the bistable collapse did not reproduce in
process in 42 attempts across four reader shapes.

---

## 3. The new finding: the send window is never sized from the path

This is the program's first measured speedup from a change of our own rather
than a repair of an existing one. The hypothesis came from the team lead.

### 3.1 The premise, confirmed in source

`ResendQueueMaxByteCount` is a fixed 2 MiB, memory-scaled only.
`ReceiveQueueMaxByteCount` is 2.5 MiB, memory-scaled only. **Neither carries
any round-trip term.**

Meanwhile the round trip *is* measured: `RttWindow` maintains a scaled estimate
and a probe interval, wired to resend timing and probe decisions and to
nothing else.

So the machinery to size these buffers by the path already exists and was
never connected to the sizing.

The per-destination ceiling is therefore the queue divided by the effective
acknowledgement round trip, shared by every flow to that destination. That
predicts 671 Mb/s at 25 ms against the reported 665, eight flows buying
nothing, no ceiling on short paths, and no such ceiling for WireGuard, which
has no per-peer reliable window at all.

### 3.2 Which cap binds

A send sequence has two caps in two units gating two different populations.
`SequenceBufferSize` (32 items) sizes the pack channel and admission: packs
handed in but not yet sent, a burst buffer. `ResendQueueMaxByteCount` (2 MiB)
bounds packs written and unacknowledged: the in-flight window.

Only the second divides into the round trip. The item cap never enters the
arithmetic.

### 3.3 The measurement

Absolute rates in a test cell are not the deliverable; the before-and-after
multiple is, because the mechanism is a ratio and a faster system sees a
similar relative speedup. That framing is what made this testable: rather than
raising a cell's throughput to reach the reported regime, raise the latency
until the regime comes down to the cell.

Adaptive window against the fixed 2 MiB, same binary, one field changed.

**Read the correction in 3.7 before these figures.** The rule as measured was
not sizing from the path: it divided delivered bytes by a resend-timing
quantity that floors at 300 ms, so it overshot and pinned itself to its
ceiling. What this arm demonstrates is that a larger window produces more
throughput at a long delay. It does not yet demonstrate that a path-derived
rule produces it.

| Carrier RTT | Transfer | Fixed | Adaptive | Paired multiple | Better |
|---|---|---|---|---|---|
| 200 ms | 16 MiB | 68.2 Mb/s | 117.6 | **1.707** (1.605-1.761) | 10/10 |
| 400 ms | 16 MiB | 35.0 | 60.0 | **1.716** (1.703-1.745) | 10/10 |
| 200 ms | 64 MiB | 70.3 | 149.7 | **2.128** (1.971-2.145) | 7/7 |

Disjoint distributions in all three. **Latency-invariant to 0.5%** across a
doubled round trip, which is the part that transfers to a faster system.

The rule's own evidence confirms it engaged: sized on every adaptive run,
computing 14-16 MiB, against a fixed 2 MiB on the other arm. The sequence's
round-trip minimum reads 200.1 ms against 200 imposed, so the delay was real
rather than a queue in the harness.

That 14-16 MiB is itself the symptom: the cell's plateau implies about 3.2 MB,
so the rule computed four to five times what the path could use.

### 3.4 The mechanism, confirmed independently without the fix

A constant queue sweep, no adaptive rule involved:

| Queue | at 200 ms | at 400 ms |
|---|---|---|
| 2 MiB | 68.6 Mb/s | 34.7 |
| 4 MiB | 117.7 | 59.6 |
| 8 MiB | 129.5 | 65.1 |
| 16 MiB | 128.9 | 65.4 |

Below the plateau, doubling the round trip halves every point. Queue over
round trip, exactly as derived.

### 3.5 The startup cost, and why the multiple grows

Solving startup and steady state from the two matched transfer sizes: the
adaptive arm reaches 164.7 Mb/s steady state against a constant 16 MiB queue's
164.1. **It reaches the same place**, paying ~100 ms more in startup to get
there. The multiple grows with transfer size because that convergence
amortises.

So the rule is correct in its destination and costs something in its ramp.

### 3.6a The multiple converges near 2.3 with transfer size

| Transfer | Measured | Predicted | n |
|---|---|---|---|
| 16 MiB | 1.707 | — | 10 |
| 64 MiB | 2.128 | — | 7 |
| 256 MiB | 2.242 | 2.268 | 5 |
| 512 MiB | 2.263 | 2.293 | **2** |

Within 1.3% of the steady-state solve at both long points, converging on ~2.3
as the startup amortises.

**The 512 MiB point rests on two repetitions**, after 9 of 32 runs were
rejected for lost goroutines. That rejection is size-correlated -- a run of
that length spends a minute exposed to another container's scheduler -- and it
hit both arms roughly evenly, so it does not bias the multiple, but it thins
precisely the point an asymptote claim rests on. Read it with that attached.

**These multiples are against the overshooting rule of 3.7**, not against a
path-derived one. They measure a large fixed window against a small one across
transfer sizes, which is a real measurement of the mechanism and not a
measurement of the corrected rule. The corrected rule's prediction differs:
converging to a sufficient window faster should raise the short-transfer end
and leave the long end where it is, flattening the curve rather than moving it.

### 3.7 Correction: the rule measured the wrong interval

`window = k x delivered` is a fixed point by construction -- if extra window
produces no extra delivery, `delivered` stops rising and the window stops with
it. No ceiling should be needed as anything but a backstop.

It does not self-limit today because delivery is measured over `ScaledRtt`,
which is a **resend-timing** quantity that floors at `RttMinResendInterval`
(300 ms). Measured in process:

| ack delay | interval used | rtt mean | overshoot |
|---|---|---|---|
| 5 ms | 300 ms | 6.69 ms | 44.8x |
| 25 ms | 300 ms | 27.41 ms | 10.9x |
| 100 ms | 300 ms | 101.93 ms | 2.9x |

So the rule accumulates up to 45x the bandwidth-delay product, hits its
ceiling, and the ceiling becomes the operative bound rather than the backstop.

**The worst case is a short path**, which is the opposite of the intended
behaviour: at 5 ms it computed a 27 MB window for a path needing 125 KB.

The correction is one line -- the sampled mean from `RttWindow.Estimate()`,
which carries its sample count so an unsampled interval cannot read as a fast
one. Not yet made; it belongs in the composite fix.

PREDICTION, recorded before it is tested: corrected, the rule converges to
2x BDP, about 8 MB on this cell at 200 ms. The constant sweep gives 129.5 Mb/s
at 8 MiB against the 117.6 the overshooting rule managed, so the corrected
rule should be FASTER, converging to a sufficient window instead of ramping
toward an excessive one.

### 3.7a The interval defect is a LATENCY defect on slow paths

We had treated the floored interval as a throughput matter: the rule overshoots,
hits its ceiling, the ceiling binds. On a slow path it is worse than that.

The window is 2 x achieved rate x 300 ms regardless of the real round trip, so
the queue it permits **never adds less than 600 ms of delay at any rate**.

Added delay per arm at a 20 Mb/s drain, as `Rtt.Mean - Rtt.Min`:

  shipping 2 MiB constant    2 MiB / rate          ~840 ms
  16 MiB constant            16 MiB / rate         ~6.7 s
  the rule as built          2 x r x 300ms / r     >= 600 ms at ANY rate
  the corrected rule         one propagation RTT

So the fix is not only worth throughput on long paths; it is worth latency on
slow ones, which is the mobile case. A slow-drain cell is measuring it.

### 3.7b A defect in the initial size, found by reading

The initial size was specified as the rule's lower CLAMP, so once set the
window could never go below it. On a 20 Mb/s last mile a 3.6 MB wide-area
initial bet would stand as ~1.4 s of queue for the life of the sequence,
because the rule could not shrink below its own starting guess.

Corrected: the initial size is the **pre-sample value only**. Once the estimate
has samples the rule may shrink to the computed window, floored at a few
packets by a minimum that already exists.

A rule that can only grow is a different object from one that tracks the path,
and the difference appears only on the paths that matter most.

### 3.8 There is no receiver-advertised window

The `Ack` carries a message id, sequence id, selective flag, tag, missing
contract id and lane version. **Nothing about remaining out-of-order
capacity.** So nothing couples a sender's window to what its receiver can hold.

On overflow the receiver evicts its NEWEST buffered items to admit an older
one, and refuses the arriving pack if that pack is itself the newest. Either
way those bytes are retransmitted.

`ReceiveQueueMaxByteCount` defaults to 2.5 MiB. Against a 16 MiB send window,
one loss on a fast path opens a hole the receiver can buffer 2.5 MiB around;
everything past it is evicted or refused while the sender keeps offering.
Correcting the interval narrows this without closing it: 2x BDP on a 100 ms
path is 6.5 MB against 2.5 MiB, still 2.6x.

The receive queue is memory-scaled and the send ceiling is not, so they diverge
further on a small-memory host.

**This is why the send rule ships default-off.** Enabling it without the
receive side would trade throughput for a loss-recovery regression.

### 3.8a The window is an admission limit, not an allocation

A prediction of mine, falsified. With the interval defect, a SHORT path is the
worst case -- at a 5 ms delay the rule used 300 ms as its interval, a 27x
overshoot, computing a 16 MiB window against a 2 MiB fixed arm, with zero
variance. So I predicted the low-latency guard would fail on memory.

The window figures confirmed exactly that. The memory did not follow:

  peak pool   1.043 vs 1.051 MiB   (the LARGER window higher in 5 of 10)
  peak heap   +0.080 MiB against a 0.656 MiB A/A band
  at 64 MiB   both deltas negative

Peak pool is **0.066 of the 16 MiB window** and 0.52 of the 2 MiB one. Both
arms held the same ~1.04 MiB because that is the loop's bandwidth-delay product
at the cell's rate. The window bounds what may be admitted, not what is held,
and a short path cannot fill it. An oversized window on a short path is
harmless *because* the path is short.

Occupancy and admission need different mechanisms, and Transfer already has
both: the per-sequence window bounds admission, and the shared pool -- charged
by queued bytes, floor-and-borrow -- bounds occupancy. So "stop admitting when
held bytes approach the share" is not new machinery; it is the pool's own
admission with a budget attached, made always-present by the
no-budget-means-floor rule.

The receive advertisement then does two jobs rather than one: loss recovery,
which was the argument for it, and a receiver at its budget throttling its
senders instead of dropping -- a window used as an occupancy signal in the
ordinary way.

That loosens the compounding argument in the design, which summed the target
times each loop's round trip across three retransmission copies and treated
that as held memory. It is a bound on admission; occupancy is lower wherever
the path is short.

### 3.8b The slow-drain falsifier fired: the concern closes for TCP

60/60 valid. At a 20 Mb/s drain:

  arm                      predicted added delay   measured
  2 MiB constant           ~840 ms                 2.3 ms
  3.6 MB constant          ~1440 ms                2.9 ms
  16 MiB constant          ~6700 ms                2.6 ms
  the rule as built        never below 600 ms      2.5 ms

Peak send queue: **0.019-0.021 MiB**, a hundredth of the window or less, and
identical whether the window is 2, 3.6 or 16 MiB. Occupancy is SMALLER under a
slow drain (0.02 MiB) than an unlimited one (0.59 MiB) -- the opposite of what
a queueing argument predicts.

THE MECHANISM: end-to-end TCP flow control through the tunnel. A slow carrier
closes the client tun's receive window, the origin's TCP backs off, and the
transfer layer is never handed more than the path can carry. A window cannot
cost memory it is never given, and a floored interval cannot cost latency
through a queue that never forms.

So the composite memory bound is not merely conservative, it is largely
inoperative for TCP: the budget is not what keeps a TCP flow bounded, TCP is.
The budget's job is the cases where that protection does not exist.

**THE ONE ROUTE LEFT IS UDP**, which has no end-to-end flow control, so a
source that outruns the drain has nothing to back it off. This program's own
cells show UDP does not share TCP's ceiling (1.39 Gb/s against 0.3), so a UDP
source is exactly what can outrun a slow drain. Being built. It is also the
only cell that can confirm the initial-size clamping defect of 3.7b, since in
the TCP cell nothing fills the queue and a rule that can shrink is
indistinguishable from one that cannot.

### 3.8d UDP, traced from source: nothing backs it off

The UDP return path admits non-blocking at every stage and drops immediately
beyond: per-shard return channels bounded at 256 items, a zero write timeout
for non-TCP items, `Pack` refusing at timeout zero, and `retryReturnSend`
covering only TCP socket items.

So for UDP: memory is bounded by the window, and **latency IS the window** --
window over drain rate -- for every admitted datagram, and for every TCP packet
of the same client, since both share the per-destination FIFO sequence.

That last clause is the sharpest consequence in this section. **A UDP source
can inflate latency for an unrelated TCP flow to the same destination**, and
the delivery term is what stops it.

Stated plainly: the budget, the advertisement and the delivery term are
load-bearing for UDP and inert for TCP on a slow drain. A test asserting the
window bounds TCP memory would pass for the wrong reason -- TCP bounds itself
-- and would keep passing with the rule removed entirely.

The advertisement does reach UDP (same sequence, lane zero), but it bounds the
receiver's MEMORY, not latency. The only latency bound is the delivery term,
which drops the excess at admission.

### 3.8d-bis UDP is flat too, and no window has ever been filled

48/48 valid, ~97 Mb/s offered into a 20 Mb/s drain:

  arm                   predicted added delay   measured
  2 MiB                 840 ms                  1.7 ms
  16 MiB                6.7 s                   1.8 ms
  rule as built         never below 600 ms      1.6 ms
  old clamping, 3.6 MB  ~1.4 s                  1.6 ms

86.7% loss in every arm; peak queue 0.016-0.020 MiB regardless of a 2, 3.6 or
16 MiB window. **Every drop is at ReturnSend** -- the downstream client send
buffer refusing admission -- with zero at the ingress handoff and zero at the
return queue.

That confirms the source reading and refutes the prediction drawn from it. "A
non-blocking admit with a zero write timeout" is precisely a mechanism that
**cannot build a queue**: the excess becomes loss, not delay and not occupancy.

So both protocols are flat for different reasons -- TCP because the origin is
backed off before the transfer layer sees anything, UDP because the excess
arrives and is discarded. **No configuration measured has ever handed a 16 MiB
window 16 MiB.**

CONSEQUENCE FOR THE FIX: the budget, the advertisement and the delivery term
are not demonstrated to protect anything in any cell run. Their justification
is structural, not measured, and this report says so rather than carrying rows
of predicted harms that have all been falsified.

WHAT SURVIVES: the throughput result, which was measured rather than predicted
-- 1.7x to 2.3x at long round trips -- and the interval defect, which is real
and whose correction converges to a sufficient window instead of ramping toward
an excessive one.

The initial-size clamping defect (3.7b) is real by reading and now
**unconfirmable by measurement**, since nothing fills a queue in either
protocol.

### 3.8d-ter The inference error, twice, in the same direction

Both falsified prediction sets came from accurate source readings. On TCP the
step assumed a sender fills its window; on UDP it assumed the excess queues.
Each time the code had just been read correctly and the step from mechanism to
consequence **added a queue the mechanism excludes**.

The form worth carrying: before predicting delay or occupancy, identify where
the bytes would have to wait, and check that the code has somewhere for them to
wait.

### 3.8e One direction still owed a reading

The mechanism that closed the TCP concern is the RECEIVE side's, on download:
the client tun's receive moderation sizes its window from bytes copied per
round trip, so a slow carrier closes it and the origin backs off.

Upload is not obviously the same. There the inner flow control is the NAT's
advertised window -- sized from upstream backpressure, not the carrier --
beside a congestion window that grows without loss on a reliable carrier. So
either something bounds it that source reading has not found, or upload at a
slow drain fills the client's queue to min(window, the 4 MiB tun send buffer).

**The cell cannot test it.** `pumpEgress` reads the source's tun and calls the
provider's receive path directly: there is no client send buffer, no send
sequence and no resend queue on the source side, and source packets never cross
the paced carrier. Measuring upload needs the source rebuilt as a real client,
which is a structural change rather than a knob.

So every result in this sequence is **download only, both protocols**.

### 3.8f A sixth layer the enumeration missed

The unreliable carrier has its own flight controller: slow start and additive
increase between 8 KiB and 256 KiB. At 25 ms that is **84 Mb/s -- the tightest
ceiling in the whole chain** -- and it is the same defect shape as every other
constant, a fixed pair with no term from the path.

It did NOT bind the UDP cell: that cell reached 99-100 Mb/s at an unlimited
drain, above the 84 Mb/s an 8-256 KiB controller allows at 25 ms, because its
carrier is in-process gateway transports rather than an unreliable carrier. It
would matter for a cell with a real unreliable carrier.

### 3.8c Three instrument faults, two of which would have inverted the result

1. **The pacer paced to timer granularity, not rate.** A frame at 20 Mb/s is
   due in ~450 us, below runtime granularity, so per-frame sleeping capped the
   link near 8 Mb/s against a configured 20.
2. **The carrier's own route absorbed the backpressure.** The default
   1024-frame route is a third of a second of buffering at 20 Mb/s: 785 frames
   sat in the wire while the send queue held 0.03 MiB and the RTT minimum read
   510-565 ms with no delay imposed. Caught only because wire-held frames were
   recorded SEPARATELY rather than folded into occupancy.
3. **Occupancy was inferred from the global pool**, which charges each root its
   whole size class and counts tun, NAT and origin buffers. It read 1.044 and
   1.056 MiB for a 2 MiB and a 16 MiB window -- identical, and identical to the
   no-delay guard at full speed. That coincidence exposed it.

All three are the shape this program has now seen seven times: something
accurate standing in for the record that decides. The pool count was accurate
and uninformative; the wire's buffering was real and not the queue; the timer
slept exactly as asked and not as intended.

### 3.9 The next ceiling, at ~4 MiB, identified

Both sweeps plateau from 8 MiB upward. It is a *window*, not a rate: 129.5
against 65.1 is a factor of 1.99 across a doubled delay, with bytes in flight
agreeing to 0.6%. A service-rate limit would give the same Mb/s at both.

It is **not** the H3 stream window, which was our first attribution and which
we withdrew. Two reasons: 3.24 MB of goodput in flight cannot come from a
3.15 MB framed window, since goodput per framed byte is below 1; and the test
cell's carrier is in-process, so no QUIC stream and no carrier socket exist in
the measured path at all.

**IDENTIFIED: the tun's own send buffer at its 4 MiB cap.** Both opposite
predictions hit, 32/32 runs valid, disjoint distributions:

  tun default, origin default   154.5 Mb/s   ratio 1.000
  tun default, origin reduced   154.0        ratio 0.997  (predicted 1.00)
  tun halved,  origin default    79.6        ratio 0.515  (predicted 0.50)

Effective window is 0.92 of the cap at 4 MiB, 0.95 at 2 MiB.

The origin's kernel socket is excluded twice over: it sits on the
provider-to-origin loopback loop, not the delayed loop, so it cannot be a
200 ms window; and its default had already autotuned ABOVE the reduced request
while the plateau still tracked the tun. (An explicit SO_SNDBUF disables
autotuning and Linux doubles the request, so the origin arm was a 20% cut
rather than a halving. The exclusion survives it: a 50% tun cut moved
throughput 48.5%, one-for-one, so a 20% cut to a binding origin socket should
have moved ~20%. It moved 0.3%.)

The 0.845 framing factor is confirmed by measurement at 0.8605 against a
derived 0.865, agreeing to 0.5%. The stated frame arithmetic gives 0.901
rather than 0.865, so the conclusion is confirmed and the frame size in the
derivation is not.

A related finding stands on its own: `H3MaxStreamReceiveWindowByteCount` is a
fixed 3 MiB that memory scaling can only lower, and quic-go grows toward it
from 256 KiB but never past it. That is the **same defect shape as the resend
queue, one layer down** — growth is path-driven, the ceiling is a fixed byte
count not derived from the path. On a real H3 carrier it would bind at 3 MiB.

---

### 3.10 Why every cell was flat, and where occupancy could ever approach a window

The fact that explains both falsifications, present in the design's own record
and not applied either time: **the sequence goroutine writes a Pack to the
carrier BEFORE it enters the resend queue.** So the queue holds only what the
carrier has already accepted. Against a carrier accepting at the drain rate
that is 20 KiB, and no window above it is ever reached. Both cells were flat,
and flat at the same figure, for this reason.

It follows that occupancy can approach the window only where a layer below the
sequence accepts faster than the far end drains. Neither carrier here does:
the reliable one accepts only what its congestion window allows, an autotuned
kernel socket about twice its own BDP, and no carrier socket is pinned. **The
one such layer on the production path is the platform relay**, whose bound
lives in the server tree -- the harness's 1024-frame route buffer was a model
of it.

A fast long-RTT wire fills the window in flight, but that is the throughput
case, not a harm.

### 3.11 The one constructible harm: multi-route reordering

With a single reliable carrier a loss is a **contiguous tail**, resent in
order, so the receiver's out-of-order hold is never used. That is why no cell
has exercised it.

With **two routes**, a route death leaves a **scattered subset**, the hold must
buffer around the gaps, and beyond 2.5 MiB it evicts.

A two-route failover cell is being built. Predictions recorded before it runs:

  without the advertisement      ~2 MiB evicted and retransmitted
  with it                        zero
  at the shipping 2 MiB window   zero, by an accident of ordering
                                 under the 2.5 MiB hold

That third row is what makes it a test rather than a demonstration: the
shipping configuration is safe by **luck, not design**, so the harm appears
only once the window grows past the hold. Which makes the advertisement not a
fix for an existing bug but **a prerequisite for the ceiling raise being
safe**.

If the first row shows no eviction, the advertisement has no measured
justification and should not land.

### 3.11a MEASURED: the harm is real, and past the threshold the transfer stalls

Two real clients, two routes, 50 ms, one route killed halfway through a 20,000
message transfer so its in-flight frames are lost as a scattered subset. 15/15
valid. Hold capacity 2.500 MiB.

  send window        delivered    peak hold   % of cap   drops    resend
  2 MiB (shipping)   20000/20000  0.216 MiB   8.6%       0        759 KB
  3 MiB              20000/20000  2.500 MiB   100%       804      2,042 KB
  4 MiB              20000/20000  2.500 MiB   100%       7,729    12,810 KB
  8 MiB              13,526       2.500 MiB   100%       16,183   454 KB
  16 MiB             11,295       2.500 MiB   100%       44,831   690 KB

**Past the threshold the transfer does not complete.** The resend column falls
at 8 and 16 MiB because the stream stalled early and less was ever sent, not
because less was lost.

The shipping window is safe **by ordering**: it sits below the hold, so the
hold cannot be overrun. The threshold falls between 2 and 3 MiB, exactly where
a 2.5 MiB hold says it should. That is now measured rather than argued.

**THE RULE CONVERGES TOWARD 16 MiB -- the worst arm here.** As built it would
move a client from the safe configuration into one where a single route death
costs a stalled transfer. So a bound keeping the send window under the peer's
hold is a **precondition** for raising the window, not a refinement: the
ceiling raise and the advertisement land together or neither lands.

### 3.11b The tree cannot count the thing that matters

`receiveQueueDropCount` counts arrivals the hold could not admit. **Eviction --
removing an already-held later item to fit an earlier arrival -- has no
counter.** So drops are a lower bound on hold pressure, and retransmission
cannot separate the two because both end in a resend.

That is why this harm was invisible in every campaign until a cell measured
peak occupancy against capacity directly. The counter is being added.

Two structural facts established while building that cell, both limiting
earlier results: the zombie cell's peer is **synthetic** -- it unmarshals a
pack, delivers the packet and calls `sendAck` itself, with no receive sequence
and no hold -- so that cell could never have shown this. And its source side
calls the provider's receive path directly, so the upload arm is a rebuild
rather than a knob and remains untested.

### 3.11c The stall is SILENT RENEGING, not a sizing problem

Traced from source. A selective acknowledgement does **not release the item**.
`receiveAck` marks it `selectiveAcked`, sets its resend time to send time plus
`SelectiveAckTimeout` (60 s), and re-adds it to the resend queue. Every resend
path then **skips** it -- the paced resend, gap recovery, and the carrier-change
resend that fires on route death. The flag clears only on the timeout resend,
60 s later.

**And eviction sends nothing at all.**

So when the hold evicts a held item to admit an earlier arrival, the sender is
holding a sixty-second lease on bytes the receiver no longer has. That is
silent reneging, and it is why the transfer stalls rather than merely
retransmitting: at 8 and 16 MiB the evictions dominate and nothing completes.
At 3 MiB the refused items were never held, so they resend on the ordinary path
bounded by a four-per-scan gap burst -- 800 drops, completed.

**The harm is therefore not that a window is too large for a hold.** It is that
a receiver can silently discard bytes a sender believes are delivered, with a
minute-long timeout as the only recovery.

THE FIX IS TWO FIELDS, not one:
  - `Ack.receive_window_byte_count` -- capacity **from the delivered point**.
    (A correction to the design's own earlier figure: "share less what it
    holds" double-counts, since held bytes are already inside outstanding, and
    would pull the right edge inward as the hold fills.)
  - an **eviction notice** carrying the ids removed, on the next ack. The
    sender clears `selectiveAcked` and resends at once, on the unbounded
    carrier-change path rather than the four-per-scan gap burst.

Gap structure need **not** be advertised. Because a selective ack does not
release the item, held bytes are at most the sender's outstanding-from-
delivered, so a sender keeping that under the advertised capacity can never
force an eviction -- one gap or a thousand. The ordinary rule, no new concept.

PREDICTIONS for the rerun, recorded first: with both fields, zero evictions,
zero refusals, completion at 8 and 16 MiB, a dip of one round trip. With the
notice alone, completion at every window with one round trip per eviction
generation instead of a minute. The second isolates which field does which job
and shows what a legacy sender gets when only the receiver is upgraded.

### 3.11d The ordering inverts asymmetrically, and that is ordinary production

**Symmetrically it never inverts.** Both shipping constants scale by the same
factor from the same 64 MiB reference, so on one host the hold is 1.25x the
window at every budget, floors included. (My earlier claim that they diverge
was wrong; it was about the delivery-sized rule's ceiling, not the shipping
constant.)

**Between hosts it does.** A provider runs unbudgeted, so its window is 2 MiB.
A client at budget B holds `max(320 KiB, 2.5 MiB x B/64 MiB)`. So the hold is
below the peer's window whenever **B < 51.2 MiB**:

  client budget 24 MiB   hold 960 KiB against a 2 MiB window   2.1x over
  client budget 32 MiB   hold 1.25 MiB                         1.6x over
  at or below 8.2 MiB    hold at its 320 KiB floor             6.4x over

The general condition is a receiver budget below 0.8 of the sender's. Upload is
safe. **Every mobile budget this program has discussed is inverted**, so the
60-second reneging is reachable on main today with none of our changes.

**The trigger is ordinary.** Under the production Auto policy the second
carrier is dialed 2 s after the first regardless of the first's health, its
routes register beside the first's with no standby guard, and ordered streams
are offered to reliable routes in shuffled order taking the first that accepts
-- striping. Two live routes is the default steady state. A route death is a
handover, an extender rotation, an idle drain, or a middlebox closing the
second carrier.

A second candidate needs no second route at all: on hybrid carriers, download
frames below a size threshold ride the datagram lane while the excess above the
flight limit takes the stream lane, so one datagram loss opens a hole the
stream lane runs ahead of.

**REPRODUCED ON UNMODIFIED MAIN.** Connect main, none of this program's
changes, provider unbudgeted at a 2.000 MiB window, client budget as the axis,
both values recorded RESOLVED per run:

  client budget   resolved hold   inverted   hold saturated   completed
  8 MiB           0.312 (floor)   6.40x      2/2              0/2
  24 MiB          0.938 MiB       2.13x      4/4              0/4
  52 MiB          2.031 MiB       no         0/2              2/2

At 24 MiB: 13,111-18,199 arrivals the hold could not admit, 11,780-15,068 of
20,000 delivered before progress stopped. At 8 MiB, about a quarter delivered.

**The 52 MiB control is what makes this a defect rather than a demonstration.**
Its hold still reaches 84-90% of capacity -- the path is working it hard -- and
it takes zero drops and completes. The only variable is whether the hold
exceeds the peer's window.

**The trigger is small.** The dying route took 20, 71, 215 and 355 frames in
the four failing runs. What fills the hold is not the loss; it is everything
the peer sends after the gap, and the peer is entitled to its whole window.

**And it needs no failure at all.** The general condition is any gap persisting
longer than hold / rate -- 75 ms at a 938 KiB hold and 100 Mb/s. **Two live
routes of unequal latency produce that with nothing failing**: a frame striped
onto the slower route arrives late, everything sent after it on the faster
route arrives first and fills the hold, and the gap outlives the hold's
capacity. On a phone with one carrier on cellular and another on wifi that is
the ordinary case. A cell is sweeping the latency difference to find the
threshold.

A variance on the record: in a campaign running all three budgets sequentially
in one process, the 24 MiB arm completed 2/2, where alone it failed 4/4. The
budget is restored between scenarios and each builds fresh clients, so this is
process state and timing -- the 8 MiB scenario ahead of it stalls for a minute
and leaves a loaded runtime, so less is in flight at the kill. **Incidence is
quoted from isolated runs only.**

Two instrument facts: the perfvar package cannot compile against main, since it
uses instruments this program added, so the failover cell now lives in its own
self-contained package with a driver flag -- a reusable capability for any cell
that must run on both trees. And the send-stats reader does not exist on main,
so peak hold against resolved capacity is the reading and the resend columns
were corroboration on the branch only.

### 3.11e Guards, in landing order

1. **Receiver-side, no wire change**: the hold's floor becomes the peer's
   unscaled window. Removes the inversion at its source. **Memory cost on the
   constrained side**: a phone at 24 MiB goes from 938 KiB to 2 MiB, +1.1 MiB
   against a ceiling this program deferred.

   **SUPERSEDED by 1a.**

   1a. **NEVER-EVICT, and it is sound.** Refuse the arriving item rather than
   remove a held one. No memory cost on the constrained side, no wire field,
   no coordination; protects an updated receiver against any peer.

   The head-of-line objection dissolves on one line of the receive path: an
   arrival AT the delivery point is delivered directly -- the branch where the
   sequence number equals the next expected registers the contract, delivers,
   and returns -- and only an item BEYOND the delivery point reaches the queue
   -and-evict branch. **So the filler of a hole never needs hold space, and a
   gap behind a full hold always fills.**

   A refusal acknowledges nothing, so selective acks stay truthful, and the
   sender resends on paths that already exist: a dead route's frames on the
   carrier-change path (prompt, not burst-bounded), a gap with held items
   beyond it on gap recovery (those held items are its proving acks), the
   refused tail on the paced resend at the 300 ms floor. Each fill drains the
   prefix and frees space.

   Recovery: one round trip plus a few paced intervals -- **under a second at
   50 ms, against sixty**. The cost is redundant resends, bounded by a window
   per round and that 300 ms floor, which the advertisement later removes by
   removing the refusals.

   Caveat for the code comment: the hold keeps what arrived FIRST rather than
   what is earliest in sequence, so under sustained reordering recovery
   lengthens by rounds. Never by a lease.
2. **Sender-side, deployable on providers alone** to protect phones already in
   the field: a carrier change voids selective acknowledgements, so the
   carrier-change resend covers evicted items and a minute becomes a round trip.
3. **The two fields of 3.11c** -- the complete fix, and the precondition for
   any window above the hold.

### 3.12 The landing, sized to the evidence

| Group | Contents |
|---|---|
| **Measured, lands** | Raise the transfer, tun and H3 ceilings consistently (H3 pending the namespace cell). The 1.7-2.3x throughput result |
| **Structural and cheap, lands** | The interval and floor correction; the mandatory budget (no-budget-means-floor). A rule that measures a resend timer instead of a round trip is wrong on its own terms, whatever its consequences |
| **PRECONDITION, lands with the ceiling raise** | The receive advertisement. The two-route cell measured a stalled transfer past 3 MiB, and the rule converges toward 16 |
| **Deferred** | Proportional occupancy division and pooling, until a regime produces the need |

### 3.13 The configuration surface is two elements, not three

The assumed round trip was the last configured constant, and it is gone.

The harness measured it and found it is **not a property of the path**. It is
bounded by the peer's hold over the target: 21 ms against an unbudgeted
provider, 7.9 against a phone at 24 MiB, 2.6 against a phone at the floor. A
blind sender must assume the worst peer and take 2.6 -- the expensive end of a
startup curve where a 1 ms bet costs 5.64 s against 0.30 s on a 100 ms path.

**But a sender need only be blind for one round trip.** Before the first ack it
assumes its peer holds at least the receive hold's floor, 320 KiB, which every
receiver already ships -- a constant the RECEIVER owns, not one configured on
the sender. After the first ack it knows the peer's hold exactly, and that is
the largest harmless window, because permission is not occupancy and the one
harm of an oversized window is the overrun the advertisement bounds.

Two mechanism changes make the jump possible:

  - the initial becomes the advertised capacity **the moment it is learned**,
    as a step rather than a target to climb toward;
  - the delivery cap becomes **one-sided and lagged** -- it may lower the
    window, never raise it, and acts only on delivery measured over a full
    interval at the CURRENT window. Without the lag, the small delivery
    measured during the blind round trip would drag the window straight back
    down and reimpose the ramp.

**The blind round trip ordinarily costs nothing.** The encryption handshake
rides the same sequence and its control pack is sent acknowledged, so the
advertisement returns before the first data pack. Where data does come first, a
TCP flow's slow start needs 4-5 round trips to reach 320 KiB of congestion
window, so the transfer window is not the binder anyway. Only a UDP source at
full rate on a fresh sequence pays it -- one round trip, once per sequence,
never a ramp.

One bet, the floor, not one per role: a role-specific bet would be a constant
again.

A legacy peer gets **today's window at the sender's own scale** -- not the
2.5 MiB hold constant, which would be a raise on no evidence, and not the
floor, which would regress every peer not yet updated. The status quo, whose
stall guard 2 mitigates.

**So turning the rule on is one change rather than three that must agree.**
Three knobs that must agree is how the present defect arrived.

### 3.14 The underlying issue: a memory budget that can only scale DOWN

The goal is symmetric window resizing in both directions, and a solution that
works in only one direction has an underlying issue. Tested against the design,
that turned out to be exactly right.

**The mechanism is symmetric by construction.** Every direction is a send
sequence at one end and a receive sequence at the other, running the same code.
Nothing in the rule knows which end is the phone.

**What is not symmetric is the configuration, and the cause is structural.**
`memoryTargetScale` returns 1 at or above the 64 MiB reference and a fraction
below it. Every window and hold in the enumeration -- transfer window and hold,
tun buffers, H3 windows, ladder maximum -- is `MemoryScaledByteCount` of a
constant. **So all were sized for the reference host and can only shrink. A
provider with eight gigabytes runs a 64 MiB device's window.**

That explains this whole program. Every constant we unpicked was chosen for a
small host, and no amount of memory could ever raise it. The two configuration
asymmetries follow: an unbudgeted provider sits exactly at the reference, a
24 MiB client below it, and the download-only inversion is **the two ends
differing, not the rule differing**.

**The surface removes it on one condition.** The share must be a draw on the
budget, proportional to it, and **never `MemoryScaledByteCount` of anything** --
otherwise the property survives under a new name. The pools must be built from
the budget and a fraction rather than from the scaled constants they use today.
That is easy to violate by accident, because every adjacent line does the wrong
thing, so it is asserted rather than documented.

**The receive hold derives from the surface too**, or it becomes the binder at
2.5 MiB -- 90 Mb/s at 200 ms -- and the raise is inert above it. Between
advertising peers the window-under-hold relationship then holds at every pair of
budgets by construction.

**A trap this creates, and the guard against it.** Under the new scheme a share
is a fraction of a budget, and a provider is unbudgeted today. If an unbudgeted
process has no budget to draw on it falls to the floor -- a large regression
from its present 2 MiB. So the no-budget path must preserve **today's
behaviour**, not the floor. The earlier no-budget-means-floor policy was right
when the floor was a safety property; it is wrong once the share is the sizing
mechanism itself.

**What the budget honestly cannot do**, stated rather than hidden. A phone
cannot reach a 1 Gb/s target on a long path within its budget, by arithmetic:
one window at the target on a 200 ms path is 29 MB framed against 25.2 MB for
the whole process. At an 8 MiB receive share the plateau is 290 Mb/s at 200 ms,
which is the target at 58 ms and below; 145 Mb/s at a 4 MiB share. **It fails
visibly** -- the window equals the advertised capacity and the estimate names
the binding term -- rather than silently at a constant. That visibility is the
whole difference between this and what it replaces.

## 4. What is still open

| Question | State |
|---|---|
| Does the multiple keep growing with transfer size? | Running. Steady-state solve implies ~2.4 |
| Is the rule a no-op at low latency? | Queued, and now **predicted to fail**: the interval bug makes short paths the worst case (27 MB computed for a 125 KB need) |
| Does the corrected interval make the rule faster, as 3.7 predicts? | Not yet tested |
| Does the receive side need path sizing, or receiver-advertised flow control? | In design. The second is a wire change |
| What does the rule cost in memory while doing nothing? | Queued with the above |
| Which inner window sets the 4 MiB plateau? | Queued |
| Does occupancy approach the window when the drain is slower than the sender? | Being built. **The one untested memory risk** |
| Why do 8 flows give 1.84x one flow, identically with and without the Transfer layer? | Unexplained |

RETIRED: an "unattributed ceiling near 190 Mb/s" appears in earlier notes and
does not survive. The same cell runs 651-671 Mb/s at 5.4 ms. It came from older
campaigns under different configuration and was never a property of the cell.

The rule ships **default-off** until the no-op guard answers.

---

## 5. Measurement stance

**Nothing in this work depends on the reporting machine's configuration.** We
measure against our own local baseline and report relative improvement,
because a ceiling is a window over a round trip: the multiple transfers to any
system, the absolute does not.

That is also why our test cell topping out far below a production provider is
not a limitation for these questions. We can put the cell into any regime by
moving the round trip, which is exactly what produced the result in section 3.
The cell's absolute rate never needed to match anything.

One consequence worth stating for anyone reproducing this. The buffer-pin sign
in section 1.1 is genuinely host-dependent — it turns on `net.core.rmem_max`
against `net.ipv4.tcp_rmem[2]`, and the same pair on the write side. That is
why the fix is a runtime rule that reads those values rather than a constant
choice. It is not a question we need answered about any particular host; it is
a question the code now answers on whatever host it runs.

---

## 6. Method notes

Several of these cost us real time and may be useful to anyone measuring this
system.

**Seven instrument defects, in two shapes.** Four were absences dressed as
presences: a settings knob that was never connected but recorded a plausible
value; a pooled counter that erased a binary signal (`1,1,1,1,1` against
`0,0,0,0,0` read as "4 across 5 runs", i.e. noise); a remembered queue missing
two campaigns; a driver reporting success for a campaign that produced zero
records. Three were the inverse, which is worse: a per-flow rate that reported
a real effect where there was none; a correction asserted confidently without
evidence; and a delay element that was a rate limit wearing a delay's clothes.

The inverse shape is worse because a *false* finding that corroborates an
existing prediction does not announce itself, whereas a missing result
eventually does.

The forms that transfer:

- When a step reports success, check the artefact it was supposed to produce —
  **and** that the artefact contains what it was supposed to contain. An empty
  file and a file of zeros fail differently.
- Check that the quantity you are measuring is the quantity you named. Bytes
  over each flow's own duration is an accurate number and it is not a share.
- When you correct a measurement, the correction needs evidence too.
- Record what a setting *resolved to* in-process, and read that column before
  any result column.
- Read per-run values before pooling.

**An instrument needs a positive control, exactly as a fixture does.** Our
carrier delay element was correct as written and became a rate limit only
because work was placed on its timing goroutine. It now asserts its own
lateness every run and fails the run when it exceeds a proportional bound. A
one-off diagnostic would have handed the next such change a silently distorted
delay and a plausible slope.

**A bound that fires on a third of runs is not a gate, it is a bias.** Our flat
5 ms lateness bound rejected 17 of 22 runs at a 200 ms delay, including 4 of 6
of the slowest arm specifically, because the slowest arm spends the most wall
clock exposed to another container's scheduler. It now uses proportional
bounds with a *mean* term, which discriminates a backlog (mean rises with max)
from a scheduler tail (mean stays far below).

**Pool ownership was never written down, and five fixtures got it wrong the
same way.** Sequence-level entry points *take* a buffer and return it
themselves; callback-level entry points one layer up *borrow* it and return
nothing. Both are now documented per entry point, listed in `CODESTYLE.md`
under `borrows` / `takes` / `takes on success`, and there is a helper so the
correct pattern is the easy one. The fifth violation arrived *with* an
instrument fix built to remove a different artefact, which is why the
reconciliation now runs on any change that adds a channel, a queue, or a
hand-off.

**Hypotheses killed by measurement, with the instrument that killed each.**
Recorded so nobody revisits them without new evidence: a 4.5x figure that was
the harness's own serial delay element; pool pinning as the threshold
mechanism, which failed its own pre-registered falsification; the
per-destination reliable bound as a cell's figure, in a cell with no Transfer
layer; a window-cap model predicting a 64x spread that measured 3x
non-monotonically; the client receive window as the download limiter, flat
across 32x; and two separate mechanisms for an upload cliff, killed by the
absence of the signatures each predicted.
