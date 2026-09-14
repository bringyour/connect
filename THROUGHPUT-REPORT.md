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

### 3.9 The next ceiling, at ~4 MiB

Both sweeps plateau from 8 MiB upward. It is a *window*, not a rate: 129.5
against 65.1 is a factor of 1.99 across a doubled delay, with bytes in flight
agreeing to 0.6%. A service-rate limit would give the same Mb/s at both.

It is **not** the H3 stream window, which was our first attribution and which
we withdrew. Two reasons: 3.24 MB of goodput in flight cannot come from a
3.15 MB framed window, since goodput per framed byte is below 1; and the test
cell's carrier is in-process, so no QUIC stream and no carrier socket exist in
the measured path at all.

OPEN: which inner window sets it. Two candidates at 4 MiB, being separated by
halving each in turn.

A related finding stands on its own: `H3MaxStreamReceiveWindowByteCount` is a
fixed 3 MiB that memory scaling can only lower, and quic-go grows toward it
from 256 KiB but never past it. That is the **same defect shape as the resend
queue, one layer down** — growth is path-driven, the ceiling is a fixed byte
count not derived from the path. On a real H3 carrier it would bind at 3 MiB.

---

## 4. What is still open

| Question | State |
|---|---|
| Does the multiple keep growing with transfer size? | Running. Steady-state solve implies ~2.4 |
| Is the rule a no-op at low latency? | Queued, and now **predicted to fail**: the interval bug makes short paths the worst case (27 MB computed for a 125 KB need) |
| Does the corrected interval make the rule faster, as 3.7 predicts? | Not yet tested |
| Does the receive side need path sizing, or receiver-advertised flow control? | In design. The second is a wire change |
| What does the rule cost in memory while doing nothing? | Queued with the above |
| Which inner window sets the 4 MiB plateau? | Queued |
| Why do 8 flows give 1.84x one flow, identically with and without the Transfer layer? | Unexplained |

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
