# The flight gate: tests, findings and fixes

Final report on the peer review, test program and measurement campaign that
followed urnetwork/connect#208 and #209.

Written for the reporter who submitted those pull requests, and for the team.
It is self-contained. The design record is `connect/FLIGHTGATEFIX.md`, 36
sections written over four days; nothing here requires reading it.

1. What the reporter found, and got right
2. What was measured against the submitted PRs
3. The regressions in the submitted PR
4. What shipped, and what ships off
5. Product findings that belong to neither diff
6. The methodology findings
7. What is still pending
8. Provenance

## How to read the numbers in this report

One calibration result governs everything below, so it belongs at the front
rather than at the end.

Late in the program we ran the measurement pipeline against itself: two arms
built from the same commit, connect `89e1633` with sdk `5f2652f`, treated as
independent by the harness, over the same seventeen cells, five repetitions,
interleaved on the same seeds, with the same primaries. Campaign
`flightgate-aa-20260912`.

| Quantity | Value |
| --- | --- |
| paired differences, where zero is the truth | 73 |
| mean | +3.1 % |
| standard deviation | 20.3 % |
| range | −44.5 % to +75.3 % |
| cells the strict rule would have called "worse" | 15 of 17 |
| storm runs, arm A against arm B | 25 against 25, of 155 correct runs |

A null tree beats and loses to itself by tens of per cent on this rig. Every
per-cell verdict this program recorded at five repetitions is inside that band
and is retired as a verdict. What survives is the result with a stable paired
sign in every repetition and an effect larger than the band, or a rate
measured at twenty repetitions or more.

So, throughout: a figure from a twenty-repetition campaign carries weight; a
figure from five repetitions is a measurement, not a verdict, and is labelled
as such. Where a figure could not be sourced to the ledger or to a test, this
report says what the test asserts instead of quoting a number.

## 1. What the reporter found, and got right

The defect is real, the diagnosis is correct, and both pull requests are
merged upstream on connect main.

**The collapse reproduces.** Campaign `flightgate-mixed-20260911` ran the
pinned-provider shape the reports describe, a direct p2p lane beside an H1
relay, over eight profiles. Stock collapsed on every lossy direct profile:
every window of every run below the 5 Mbit/s threshold, medians 2.3 to 2.6
Mbit/s where the run completed at all, and 13 of 40 runs failing at their
workload deadline. The merged PRs removed it: zero dead windows in seven of
eight cells and medians of 18.5 to 27.1 Mbit/s.

**The mechanism is the one they named.** The route-wide admission gate, M1 in
their report, is confirmed in the source (`transferFlightPolicy().limited` is
`unreliableTransferPath`, `flightEligible` is evaluated before route choice,
and `releaseUnreliableFlight` is reached only from `receiveAck`) and confirmed
in the counters: 99.8 to 99.9 per cent of stock's flight waits happened while
a non-unreliable route had channel capacity, and both fixed arms wait zero
times. The `p2p-legacy+exchange-h1` control, which has a reliable direct lane
and therefore no unreliable flight, recorded zero dead windows in both arms on
every profile. The collapse needs the unreliable flight, not merely a lossy
direct lane. Even the clean mixed control shows 786 blocked waits per five
runs on stock, so the gate closes without any loss at all, on reordering
alone.

**On real phones it is worse than reported.** Two devices pinned to each other
as network peers, identical diagnostic builds, four-stream download, twelve
15-second windows per run, six runs per role. Every window of every stock run
was dead by the 5 Mbit/s threshold, 72 of 72 per role, at 2.5 to 3.4 and 1.5
to 2.5 Mbit/s, against the same devices downloading directly at about 65
Mbit/s on LTE and 550 Mbit/s on Wi-Fi. On a phone the gate is not a collapse
after 30 to 90 seconds. It is the steady state from the first second. Provider
side, 5,300 to 11,000 flight waits per three-minute run, 98 to 99 per cent of
them blocked while a reliable route had channel capacity, with the binding
limit the mobile ceiling of 16 messages in flight rather than the byte floor.
After the merge, provider flight waits fall from 7,142 to 11,762 per run to
zero.

**Their source claims held up under review.** Of the claims checked line by
line against this tree, the route-wide gate, the ack-carrier affinity, the
p2p-first write ordering, forced direct mode for an own-account pin, the
three-later-selective-acks gap rule with no lane awareness, the single RTT
window fed by both lanes, the ICE consent timeout and `ReliablePackHandoffTimeout
= -1` were all confirmed. Two were sharpened rather than corrected: the ack
affinity is stricter than described (the eligible set for a p2p-received Pack
is p2p alone, written by one serial ack worker per receive sequence, so a full
p2p channel head-of-line blocks every later ack), and the flight floor is 8
KiB of bytes with a message floor of 4.

**Their own A/B was credible for their rig**, 47 fixed runs without a collapse
against 9 collapses in 18 stock runs, and the regression tests added to both
PRs in the third report answered the request for failure reproductions on M1,
M2 and M7.

The rest of this report is corrections and additions on top of that. None of
it changes the paragraph above.

## 2. What was measured against the submitted PRs

### 2.1 The deterministic tests

The program added 26 files matching `flight_gate_*_test.go` on connect main
`b8f72dd`, carrying 103 test functions and 3 benchmarks. Seven of those files,
holding 26 of the 103, are behind the `flightgate_next` build tag and do not
compile against the shipped tree by construction: they reference settings a
candidate would add, and they are the specification of a candidate rather than
a suite anyone runs. So 77 tests run against the shipped tree. Outside those
files the program added `net_http_plain_websocket_test.go` (three tests) and
adapted three existing test files without adding functions.

Three groups are deliberately built from API that predates this program, so
they compile and run against the submitted tree unchanged and the record can
say which tree holds which row:

- **The lane recovery contract**, 14 rows. Rows 1 and 2 bound the writes during
  a stall to a logarithm of the stall over the interval rather than one per
  item; the submitted tree fails them by construction, because it rewrites its
  whole window. Row 3 pins that a draining lane is not rewritten, row 4 that a
  genuine endpoint drop is still recovered, rows 5 and 6 that a relay hole
  overtaken by direct acknowledgements is not written while one proven by
  relay acknowledgements is, row 8 that a late item on a draining lane is never
  written, row 9 that a proven drop waits at most one interval, and rows 10
  through 13 the stall healing, the spin bound, the liveness cadence over a
  20-second stall, and that an item carried on two lanes proves nothing about
  either.
- **The metric contract**, 6 rows, pins what the counters mean rather than what
  the transfer does. Its sharpest row,
  `TestMetricAckWriteIsAttributedToTheCarrierItLeftOn`, is section 3.3 below.
  Its honest opposite is `TestMetricRouteGenerationChangesAreNotPinnableInProcess`,
  which records what this instrument cannot pin.
- **The dead-route contract**, 4 rows, characterises the hang of section 5.2.
  Two of its three behavioural rows are identical on both trees, because every
  step of that hang is in the submitted tree's code as well as ours.

Some rows assert a trade rather than a win and should not be mistaken for
failures. `TestLaneRecoveryRow7RelayTailDropIsProbedNoLaterThanMerged` bounds a
lone relay tail drop at no later than the submitted tree's own timer would
fire. `TestLaneRecoveryProbeRttNeverExceedsTheScaledRtt` and
`TestLaneRecoveryRow11HeldItemsDoNotSpin` are the same shape: each bounds a
cost rather than claiming an improvement.

Where a test pins a count on a shipping default, treat it as a ratchet against
regression, not as a claim that the current value is correct.

### 2.2 The corrected primary, and why it was wrong first

For most of the program the mixed-route cells were scored on selective-gap
resends. That measured half of a substitution and produced verdicts in the
wrong direction.

A deferred spurious timeout that the scoreboard later writes is one duplicate
counted as a gap write. The submitted tree's whole-window rewrite of the same
item is the same duplicate counted as a timeout write. Counting only the first
compares a tree that pays in gap writes against a tree that pays in timeout
writes and calls the first one worse. The primary is now total recovery
writes, gap plus whole-window, summed over both ends, with the components kept
as diagnostics.

Rescoring one mixed-route table that way changed four of eight verdicts:

| Cell | Gap writes | Timeout writes | Total | Rescored | Original |
| --- | --- | --- | --- | --- | --- |
| clean-lan / latency-under-load | 240 → 145 | 4,813 → 97 | 5,098 → 242 | better | better |
| loss-100bp / latency-under-load | 277 → 369 | 3,159 → 71 | 3,445 → 440 | better | worse |
| loss-300bp / latency-under-load | 454 → 945 | 3,083 → 640 | 3,547 → 1,604 | better | worse |
| burst-loss / latency-under-load | 438 → 591 | 4,665 → 204 | 5,146 → 802 | worse on dead windows only | worse |
| burst-loss / tcp-parallel | 123 → 128 | 684 → 50 | 807 → 186 | better | equal |
| clean-lan / tcp-parallel | 9 → 99 | 0 → 498 | 9 → 599 | worse | worse |
| loss-100bp / tcp-parallel | 30 → 128 | 0 → 644 | 30 → 772 | worse | worse |
| loss-300bp / tcp-parallel | 61 → 78 | 1 → 7 | 63 → 91 | worse on goodput | worse |

Arrows are the submitted tree to the candidate arm `b0b04c8`, five
repetitions. On the latency-under-load cells the duplicate traffic is 4 to 21
times lower. The tcp-parallel cells do not benefit from the substitution
argument, because the submitted tree pays almost no timeouts there, and stay
worse on the corrected primary.

Then the calibration retires most of that too. On those same cells two
identical trees produced total-recovery-write ratios of 0.07, 3.4, 83 and 495,
so a 9-to-599 change at five repetitions is not evidence of anything. The
method correction stands; the numbers it produced do not carry a verdict.

### 2.3 The device series

The only evidence in this program from real hardware. Pixel 8 Pro and Galaxy
S24 Ultra pinned to each other, relay-only reached through a debug hook and
interleaved with stock from a fresh tunnel per run.

| Roles | Stock, per-run medians Mbit/s | Relay-only, per-run medians Mbit/s |
| --- | --- | --- |
| S24 on LTE to Pixel providing on Wi-Fi | 2.9, 2.8, 2.5, 2.5, 2.4, 2.3 | 6.2, 5.4, 5.5, 2.5, 2.3, 2.5 |
| Pixel on LTE to S24 providing on LTE | 1.9, 1.8, 1.6, 1.5, 1.6, 1.7 | 4.7, 4.5, 2.1, 2.0, 2.1, 2.1 |
| Pixel on Wi-Fi to S24 providing on LTE | 2.6, 1.9, 2.1, 1.8, 1.9, 1.8 | 2.6, 2.6, 2.6, 2.6, 2.5, 2.4 |

Within every interleaved pair relay-only beat stock: about twice while the
relay path was healthy, and by 0.2 to 0.8 Mbit/s after the relay path itself
degraded mid-session. Stock with the fast path live never exceeded the
relay-only run beside it.

Two things this table is not. It is the problem measured on the stock build,
not the fix measured. And relay-only is a diagnostic mode, not a proposal.

Relay-only runs show zero flight waits and 0 to 234 gap resends, but still
1,000 to 17,000 whole-window timeout resends per run. That is direct device
evidence that the timeout machinery misfires on the relay lane alone,
independent of the gate, and it is what motivated the mechanism that did ship.

## 3. The regressions in the submitted PR

This section measures the submitted PRs as the control, because they are now
merged upstream. Their costs are in shipping code rather than in a candidate,
and this is the only place they are traced to a mechanism.

Read one thing off the top: the wedge described in section 4.2 is **not** the
PR's. It belongs to a setting this program added and ships off.

### 3.1 The exchange path on a constrained link

The largest body of evidence. Low-bar matrix, mobile surrogate, upload
direction, the merged PRs against `92a37c2`'s parent as control, five
repetitions, paired per seed. Campaign `flightgate-lowbar-20260911`.

| Cell | stock | merged | merged − stock, per seed | seeds merged ahead |
| --- | ---: | ---: | --- | ---: |
| exchange-auto / 1m-down-250k-up | 132.5 | 116.6 | −21.5, −9.8, −8.9, −25.0, −7.4 | 0 of 5 |
| exchange-auto / 5m-down-1m-up | 342.7 | 302.6 | −15.5, +3.0, −9.5, −14.6, −8.4 | 1 of 5 |
| exchange-auto / 256k-down-64k-up | 24.1 | 29.4 | +48.2, +37.5, +19.0 | 3 of 3 |

Medians are kbit/s over correct runs. So the merged PRs are 7 to 25 per cent
slower on `1m-down-250k-up` with every seed slower, 8 to 16 per cent slower on
`5m-down-1m-up` on four of five, and 19 to 48 per cent faster on
`256k-down-64k-up`, where stock also failed two runs. `exchange-h3` and
`p2p-fast` are indistinguishable.

**What five repetitions carry here, stated rather than assumed.** The A/A puts
the exchange low-bar group's paired standard deviations at 2.7 to 35.0 per
cent, which needs 1 to 96 repetitions for a 10 per cent effect depending on
the cell. The magnitudes above are therefore not reliable. What survives at
this sample size is the sign, and only on the first row: five of five seeds in
the same direction is a two-sided sign test of 0.0625, which is suggestive.
Four of five, the second row, is 0.375 and is not evidence on its own. Quote
the 7-to-25 range as a range observed in five runs, never as the size of the
regression.

**The mechanism was named before the measurement, which is why this reads as
more than a slow cell.** F1's route-wide `reliableRouteAvailable` rule counts
any route whose transport is not `Unreliable`. Hybrid H3 publishes
`Unreliable`, so an H3-only client keeps the gate; but with H1 also active the
datagram gate switches off and the overflow goes to H1 TCP on the same
constrained link. That is the burst the flight controller exists to prevent,
and the regressing cells are exactly the predicted regime: H1 and hybrid H3
both active on a constrained link, which is what mobile Auto usually is.

**Status: open, and owed to you.** The named remedy is G1, a narrower rule
that overflows only when the reliable route is a different transport from the
unreliable one. It was specified, it was never built, and no later design in
this program touches it. It is live on main today.

### 3.2 The device confirmation of the same mechanism

Twelve interleaved runs on the two physical radios with direct mode forced
off, so the exchange path is isolated. In the three pairs where the relay path
was healthy, stock beat the merged PRs by 1.5, 8.2 and 18.2 per cent, mean
9.3, with a consistent sign. The other three pairs are unusable, because the
radio rather than the tunnel was the limit.

Three usable pairs is a small sample and a consistent sign across three is
worth little alone. Its weight comes from agreeing, on real radios, with the
simulator result above and with a mechanism predicted from the source before
either was run.

Provenance note: unlike every other number in this report, this one is the
device stream's result as reported and is not in
`tests/PERFVAR-MEASUREMENTS.md`. A reader wanting the run records should ask
that stream rather than look in the ledger.

### 3.3 The acknowledgement attribution defect

A correctness defect rather than a performance one, and the metric contract
makes the submitted tree fail it by construction.

The PR records the carrier of the Pack being answered rather than the carrier
the acknowledgement actually left on. An acknowledgement that takes the H1
priority companion is therefore filed under the Pack's lane, and the reply
lane is judged by a counter that cannot see it.
`TestMetricAckWriteIsAttributedToTheCarrierItLeftOn` drives exactly that write
and reads the attribution back.

It has a measurement consequence as well as a behavioural one. The
`ack_writes_h1` column read zero in every campaign this program ran before the
fix, and two independent causes produced that zero. The PR misfiles the
acknowledgement. Separately, the campaign harness's own route wrapper did not
forward `TransportType`, so the exchange lane was labelled `unknown` rather
than `h1`; the run records show this directly, for example `{'unknown': 7802}`
where post-fix arms show `{'h1': 1511}`. The harness half was ours and is
fixed, and the readout now folds the two labels so older campaigns stay
readable. Fixing either half alone would not have revealed the other.

### 3.4 The storm behaviour

Stated plainly because it is the PR's own recovery path. Route
`p2p-fast+exchange-h1`, `tcp-parallel`, download, twenty repetitions per arm
interleaved on seeds 20260910..20260929. Twenty was chosen from the A/A,
because the per-run storm rate is near one in five and five repetitions cannot
resolve that. Campaign `flightgate-175-20260912`.

| Cell | Arm | Storm runs over 200 timeout writes | Timeout writes, median and max | Median Mbit/s | Dead windows |
| --- | --- | ---: | --- | ---: | ---: |
| clean-lan | merged | 2 of 20 | 0, 2,183 | 20.9 | 0 |
| clean-lan | shipped | 0 of 20 | 0, 30 | 21.2 | 0 |
| loss-100bp | merged | 3 of 20 | 0, 3,902 | 19.5 | 0 |
| loss-100bp | shipped | 0 of 20 | 0, 36 | 19.9 | 0 |

Paired goodput, shipped minus merged: `clean-lan` better in 14 of 20, mean
+1.0 Mbit/s with a standard deviation of 3.9; `loss-100bp` better in 13 of 20,
mean +2.7 with 4.0. Pooled storm counts 0 of 40 against 5 of 40, Fisher
two-sided about 0.055.

Read the tail rather than the rate. The rate test is marginal and the standard
deviations are large; the worst run differs by two orders of magnitude, 36
against 3,902. That is the claim this cell supports. The same cells were
re-measured later on a different commit with twenty repetitions and still
showed zero storm runs, so the result carries forward across the program's
later work rather than belonging to one build.

One caution carried from the A/A: a storm run is not rare in a null tree
either. Two identical trees produced 25 storm runs each out of 155 correct
runs on the mixed cells. What the twenty-repetition comparison establishes is
the difference between 5 of 40 and 0 of 40 on these two specific cells.

## 4. What shipped, and what ships off

### 4.1 What shipped

Read from `DefaultSendBufferSettings` on connect main `b8f72dd`, not from the
design notes.

**On.** The deferred retransmit, which is unconditional in
`shouldDeferTimeoutResend` and has no setting; its backoff,
`DeferTimeoutResendBackoff: true`; the forget on retransmit timeout;
asynchronous race-commit delivery; the counters; and the removal of the fast
path's liveness reporter.

**Off.** `ReliableLaneProvenRecovery`, `ReliableTimerUsesDeviation`,
`ReliableAdmissionBoundedByDelivery`, `DeferredItemIsLateForTheScoreboard`.
Size-aware fast-path admission is not merely off, it is not in the shipped
tree at all; its only appearance is behind the `flightgate_next` build tag, as
a specification.

So the shipped behaviour is the merged PRs plus the deferred retransmit with
its backoff, the forget on retransmit timeout, asynchronous race-commit
delivery and the counters, minus the fast path's liveness reporter.

**The deferred retransmit** is the mechanism that earns its place. On
`mixed-relay-queue-inflation-3s`, `tcp`, download, five runs per state:

| Cell | Metric | defer off | defer on |
| --- | --- | --- | --- |
| exchange-h1 | median Mbit/s | 13.1 | 15.2 |
| exchange-h1 | whole-window timeout writes | 16,837 | 5,924 |
| exchange-h1 | dead windows | 4 | 11 |
| p2p-fast+exchange-h1 | median Mbit/s | 9.0 | 15.1 |
| p2p-fast+exchange-h1 | whole-window timeout writes | 22,328 | 4,746 |
| p2p-fast+exchange-h1 | dead windows | 16 | 5 |

A 65 to 79 per cent reduction in timeout writes is far outside the null band,
as is the mixed route's dead-window change from 16 to 5. Five repetitions, so
only effects that size are called. One seed of five still collapses on the
relay-only cell with the defer on, to 3.3 Mbit/s against 13.1 for the same
seed with it off; four of five are 15.1 to 16.1. That collapsing seed is the
residue the later design rounds chased, and it is the origin of the lane rule
in section 4.2.

**Removing the fast path's liveness reporter** closed a regression this
program created and then spent three design rounds finding. That reporter was
ours, not the submitted PR's. It wrote an 11-byte progress control packet
every 50 milliseconds, three repeats per change, on every fast-path receiver.
The sender of an upload is a receiver too, of the provider's acknowledgements,
so it reported on its own uplink at up to 20 packets a second. The cell-edge
queue is counted in packets: at 64 kbit/s it drains about six data packets a
second, so the reports alone were three times its drain rate and tail-dropped
the data. Cost 13 to 57 per cent on every forced-direct repetition, plus three
lost route-readiness runs.

| Cell | `d381cfa` | `66a2130` | shipped |
| --- | ---: | ---: | ---: |
| p2p-fast / 5m-down-1m-up | −13.7 % | −14.5 % | −0.1 % |
| p2p-fast / 1m-down-250k-up | −13.7 % | −23.1 % | −0.5 % |
| p2p-fast / 256k-down-64k-up | −25.0 % | −22.7 % | −2.2 % |
| paired seeds better | 0 of 14 | 0 of 12 | 25 of 55 |

Twenty repetitions on the shipped column. All three cells are inside the null
band at a sample size that would have detected a tenth of the regression.

**The exchange low-bar cells**, same provenance and sample size, are the one
place where the shipped tree looks better than the merged PRs on rate, and the
caveat matters more than the number:

| Cell | median kbit/s, merged to shipped | paired mean | Verdict |
| --- | --- | ---: | --- |
| exchange-auto / 5m-down-1m-up | 294.6 → 299.2 | +2.3 % | better, marginal |
| exchange-auto / 1m-down-250k-up | 120.2 → 125.8 | +4.4 % | better, marginal |
| exchange-h3 / 5m-down-1m-up | 436.8 → 437.5 | +0.3 % | indistinguishable |
| exchange-h3 / 1m-down-250k-up | 158.4 → 162.5 | +2.7 % | indistinguishable |

Do not quote the two `exchange-auto` rows as a rate win. Each paired mean
clears 1.96 standard errors only marginally, and the null standard deviations
they are judged against come from the A/A at three to five pairs per cell,
which is a thin null to lean on. The evidence worth citing there is the
recovery traffic: halved on `5m-down-1m-up`, 17,965 writes against 8,607, and
down 38 per cent on `1m-down-250k-up`.

### 4.2 What ships off, and the measurement against each

Negative results are most of what this program produced. Five arms were
measured and retired by the calibration (`f8a507a`, `c64442c`, `d381cfa`,
`66a2130`, `b0b04c8`). Five mechanisms were built and measured, and none of
them ships on. None is off for lack of evidence; each has a measurement
against it, and two of those measurements were made in process, before the
mechanism could cost a campaign at all.

| Mechanism | Where it was falsified | The measurement against it |
| --- | --- | --- |
| `ReliableLaneProvenRecovery`, the lane rule | campaign | 5 of 80 scenario-runs over 100 seconds with it on against 0 of 80 with it off, pooled over three cells and two independently built arms; Fisher two-sided 0.059 |
| Size-aware unreliable admission | campaign | goodput 0.8 and 2.0 Mbit/s lower with the cap on; it is not in the shipped tree at all, only its specification behind the build tag |
| `DeferredItemIsLateForTheScoreboard`, the grace narrowing | measured | it moves the write from the cheap counter to the expensive one and changes nothing on the wire; the result went the wrong way and the design was retracted |
| `ReliableTimerUsesDeviation`, the deviation timer | in process | a stall yields no samples, so an ack-sampled estimator tightens into the stall: 411 and 425 writes against the scaled mean's 347 |
| `ReliableAdmissionBoundedByDelivery` | in process | holds the resend queue below its budget as designed, but costs about twice the transfer time in every arm and is inert on the arm its own design calls the target case |

Each of those has a test that asserts it stays off and records why, so the
next person does not re-propose it silently:
`TestLaneProvenRecoveryIsOffByDefault`, `TestDeviationTimerIsOffByDefault`
with `TestDeviationTimerDoesNotCoverAnUnsampledStall`,
`TestReliableAdmissionBoundIsOffByDefault` with
`TestReliableAdmissionBoundIsInertUnderTheMobileBudget`,
`TestDeferredGraceNarrowingIsOffByDefault`, and
`TestDeliveredBytesRingIsNotRetainedWhenOff`, which also proves a disabled
mechanism costs no memory.

**The lane rule deserves its own paragraph, because it is the most interesting
failure in the program.** The rule withholds a whole-window retransmit until a
later same-lane acknowledgement proves the item lost. It is sound wherever its
release condition is reachable. A relay stall is precisely the state where it
is not: no acknowledgement arrives on that lane by construction, so the rule's
precondition is the stall's own definition, and it withholds exactly the
traffic that gets a stalled run out. The suppression is measurable and clean.
On nine wedged runs the write-to-defer ratio separates the two states with no
overlap at all, 0.60 to 0.76 with the rule off against 0.00 to 0.23 with it
on, and total recovery writes per second separate the same way, 57 to 79 off
against 1.7 to 13.5 on.

The worst case was a run on a benign link: 1 Gbit/s direct with 1 per cent
loss, 1 Gbit/s relay at 100 ms with none, no queue inflation, no blackhole, no
scheduled events at all. It wedged for 712.8 seconds with 142 of 143 progress
windows dead, wrote 30 pieces of recovery work while deferring 1,146, and
failed. Every rule-off run of that cell finishes in 3.7 seconds.

The per-window trace is the artefact worth keeping, because "1.4 Mbit/s"
suggests a slow transfer and it was not one. A wedged run moves nothing for
two to four minutes and then releases and completes at the cell's full rate,
17 to 18 Mbit/s, in the last two or three windows. Nothing degrades. Something
holds, and then stops holding.

The rule ships off. The default flip was pre-registered in the design record
as a one-commit revert if the deciding cell came back the other way; it did,
and the revert was taken. The mechanism stays in the tree because nothing here
says it is wrong, only that it must not run unattended. What it needs is a
bound that does not depend on the lane it is suppressing, and the current
design for that is per-item and positional rather than per-route and
silence-keyed.

## 5. Product findings that belong to neither diff

Four findings surfaced that are nobody's diff. They are recorded here at the
same rank as the rest, because none of them is made smaller by having no
author in this change.

### 5.1 The relay routinely stops acknowledging for multiple seconds

On the storm cells the relay goes 2.75 to 2.9 seconds without advancing its
cumulative acknowledgement, in every run of every arm, storm or not. On the
relay queue-inflation cell the per-run lifetime maximum is 10 to 36 seconds,
in every run of every state. The worst storm observed, 3,902 timeout writes,
is also the run with the longest stall, 7,023 milliseconds.

The sender's timer cannot cover that. `RttMinResendInterval` is 300 ms, the
floor once round-trip samples exist; `MinResendInterval` is 2 s, the cold
floor with no evidence; `MaxResendInterval` is 8 s, the ceiling. Observed
reads span 0.30 to 8.00 seconds with a median per-side maximum of 2.00 to 2.06
seconds.

**A correction the report owes its own record.** This program repeatedly
described the resend timer as having a 2.0-second ceiling, because that is
where a typical run's maximum lands. It is the cold floor binding, not a
ceiling. Against the real 8-second ceiling the relay stall is one and a
quarter to four and a half times the timer's maximum wait, not the three to
four times stated in the design record, and the claim that "the ceiling sits
below the stall" is wrong as a general statement; it held only where the
2-second floor was binding. What survives is narrower and still worth acting
on: the relay routinely stalls for longer than the sender's timer will ever
wait once the floor binds, and no sender-side timer change reaches that. It is
an exchange-path finding and it is unchanged by anything in either diff.

### 5.2 A seventeen-minute hang from upstream churn

Three runs of one cell, `exchange-h3 / 256k-down-64k-up`, failed at the
workload stage after 1,018 seconds each, with the same signature: `join Pack
boundary after unstable candidate: upstream changed`. Their counters are
unlike any other run in the program: 2,528 to 3,051 whole-window timeout
writes with zero gap writes and zero deferrals, and a cumulative-acknowledgement
gap of 1,014 seconds, meaning the relay never acknowledged again for the whole
run.

**It is a pre-existing condition in a third layer, and both diffs leave it
untouched.** Traced to source: provider churn during the join leaves a
single-lane route that never acknowledges again. On that route the hybrid
transport publishes `Unreliable`, so `reliableRouteAvailable` is false for the
whole run, and `observeUnreliableResendTimeout` counts, halves and returns
before either the release or the forget in both trees. The send path is
identical on both trees, byte for byte, in this state. TCP-socket recovery-mode
flows set `retainAfterAckTimeout` on every Pack deliberately, so that a slow
provider does not tear down a flow whose own TCP recovery owns its lifetime;
those items skip the 60-second exit and the timer rewrites them at its 8-second
cap indefinitely. The layer that owns the flow's lifetime, the multi-client,
has the signal (a send stall past its 3-second bar) and reached the verdict,
and holds it: `markStallHoldOnce`, "no receiving sibling: uplink unproven". On
a route with one lane there is never a receiving sibling, so the hold is
permanent by construction on exactly the route where it matters most. The same
hold, with the same counter, is in the submitted tree.

An alternative explanation was checked and excluded: `missing_contract_write_count`
and `missing_contract_request_count` read zero in all three hung runs, so a
contract-missing loop is ruled out and the path was dead.

**On rates, honestly:** three failures in thirty-three runs on our arm against
zero in thirty-three on the merged tree and zero in ten in the A/A. That is not
significant, and a 65-repetition rerun on a fresh seed set is queued to decide
whether the rate differs at all. The mechanism does not depend on the rate.

The program's contribution here is the mechanism traced to its source lines,
four deterministic rows in the dead-route contract (the condition is visible,
the retention holds as designed, every rewrite carries the head so a receiver
that returns can resume, and the retirement decision is placed with the
multi-client rather than the sequence), and one counter. The decision belongs
where it is.

This finding sits beside your own M6 rather than apart from it. Your report's
dead-lane theme, that a lane with no liveness signal is never retired, is the
same defect one layer down: a dead route with no sibling that is never
retired. It tells us where the next liveness signal has to live.

### 5.3 A receiver-side deadlock

A receiver holding `ReceiveQueueMaxByteCount` and blocked at a hole cannot
evict anything to fit an arrival above everything it already holds, so it
drops that arrival and sends no acknowledgement for it. That drop destroys the
acknowledgements any lane-proof rule depends on, and until this program it had
no counter. `TestReceiverBudgetDropsArrivalsAboveAHole` pins it and
`ReceiveQueueDropCount` exports it.

The honest half of the finding is the negative one.
`TestReceiverBudgetDropsDoNotWedgeEitherArm` records that at a budget tight
enough to wedge, both arms wedge at about half the runs, so that wedge is not
the lane rule's. With a budget-enforcing receiver the wedges that appear in
process are not rule-attributable, and their write-to-defer ratio is the
opposite of the campaign's wedged signature: 1.97 to 3.22 in process against
0.00 to 0.23 in the campaign, an order of magnitude apart and not overlapping.

### 5.4 The mobile memory ceiling is breached on every build measured

Across seventeen MEMSTEADY blocks the 24 MiB Go-runtime ceiling, which is the
accepted iOS allocation surrogate, is exceeded on every build measured. The
worst of the three is the pre-merge shipping code, at worst quiet samples of
28.67 and 28.45 MiB with 165 and 78 samples above 28 MiB. So the overshoot is
a pre-existing product defect, not something this program or either diff
introduced.

Two structural facts explain why it cannot be fixed by tuning a budget. The
mobile budgets that retain bytes sum to 6.8 MiB, 27 per cent of the target,
while the device measures the Go runtime at 23.7 to 24.4 MiB, so roughly three
quarters of the envelope is outside every constant a sizing test can guard:
goroutine stacks, the netstack's own buffers, the QUIC and WebRTC stacks, live
packet ownership in flight, and the runtime's own heap slack and GC headroom.
And the mobile idle reclaimer never ran in any quiet window in any block: its
gate is an outstanding-owner count of sixteen
(`mobileIdleMemoryMaxOutstandingPoolCount`), while the trim it guards only
drops free-list buffers and never touches buffers a consumer holds. A connected
tunnel at idle legitimately holds hundreds, so the gate can never be satisfied
and the reclaimer defers forever. That is the root cause to fix, and this
program changed no code for it.

Six sizing invariants were added so the constants that decide whether the
tunnel trickles or crashes a phone can no longer drift apart silently across
two repositories, four in connect and two in the sdk.

### 5.5 A cellular-to-cellular regime where the limit sits below Transfer

One LTE-to-LTE stock run in which the fast path never negotiated still ran at
2.5 Mbit/s over the legacy SCTP lane with the flight never waiting. On the
Pixel-on-Wi-Fi to S24-on-LTE role the relay-only control is flat at 2.4 to 2.6
Mbit/s across all six runs. And mid-session the relay path itself degraded to
2.0 to 2.6 Mbit/s and stayed there while direct LTE on the same device stayed
at 61 Mbit/s.

So on that radio pairing there is a cap below Transfer that no change to the
flight gate, the recovery path or the timer will move. It is the platform
path, not a Transfer mechanism, and any future measurement on cellular-to-
cellular peers has to establish where that cap is before it can attribute
anything to the tunnel.

## 6. The methodology findings

These may be the most transferable part of the program, and they cost the most
to learn.

### 6.1 Calibrate the instrument before judging a candidate

The strict bar this program applied for most of its life was "not worse than
the control on three primaries in every one of seventeen cells, at five
repetitions". The A/A shows no tree can clear that bar, including the control
itself: with zero real difference, fifteen of seventeen cells would have been
called worse.

The consequences are concrete. Four candidate arms were reported as failures
on the strength of per-cell deficits inside the null band. Two design rounds
were spent explaining magnitudes that were noise; the mechanisms those rounds
traced remain true as source facts, but the claim that they explained the
verdicts is retracted. A storm rate that one round attributed to a candidate
turned out to be what the control does to itself at the same rate, 25 runs in
155 in each of two identical arms.

What survives, and only this: stock's collapse against the merged PRs;
`d381cfa` and `66a2130` on the three `p2p-fast` low-bar cells; `b0b04c8`
clearing those same cells; the deferred retransmit's relay-only A/B; and the
storm elimination at twenty repetitions.

The A/A also produced the sample sizes the rig actually needs, which is the
part worth carrying to the next program:

| Cell group | SD of paired % | repetitions per arm, 10 % effect | 20 % effect |
| --- | ---: | ---: | ---: |
| p2p-fast low-bar (3 cells) | 3.7 to 11.6 | 2 to 11 | 1 to 3 |
| exchange low-bar (6 cells) | 2.7 to 35.0 | 1 to 96 | 1 to 24 |
| mixed tcp-parallel (4 cells) | 12.3 to 40.8 | 12 to 131 | 3 to 33 |
| mixed latency-under-load (4 cells) | 9.1 to 27.3 | 7 to 59 | 2 to 15 |
| all cells pooled | 20.3 | 33 | 9 |

For a rate rather than a median, about 20 repetitions separate "no storms"
from one in five, and about 60 separate 20 per cent from 5 per cent. Those two
numbers are why the storm result in section 3.4 is reported at twenty and not
at five.

### 6.2 An estimate may pace, but it must never decide

Three candidates fell to the same thing, and the pattern only became visible
after the third.

The delivered-bytes admission bound took a decision from a count of bytes
delivered over the last scaled round trip. The deviation timer took a decision
from an exponentially weighted deviation of round-trip samples. The silence
verdict took a decision from the interval since the last acknowledgement. Each
was wrong for exactly the interval in which its estimate could not yet have
learned what it was estimating, which is what estimating means. An
alive-but-slow lane whose queue depth grows by D pauses its acknowledgements
for D, and for that D it is observationally identical to a stall of D, so any
rule that acts differently in the two regimes is wrong in one of them for the
gap's duration, whatever its threshold.

The two candidates that were closed were closed by a quantity the sender
observes directly, a later same-lane acknowledgement or the count of items a
route holds, or by a bound that estimates nothing. The rule to carry forward:
**no decision on the recovery path may depend on an estimate during the
interval it cannot yet have learned. Decisions rest on proofs, counts or fixed
bounds; an estimate may pace, never decide.**

The corollary, learned the same way: a fixed constant that cannot lag can
still fire while a lane is demonstrably draining. Replacing an estimate with a
constant is not the same as removing the estimate from the decision.

### 6.3 Falsify in process before spending a campaign

A campaign on this rig costs hours and, at five repetitions, cannot answer the
question anyway. Two designs were built to specification and killed by a
deterministic in-process reproduction before they reached the harness: the
delivered-bytes admission bound and the deviation timer. Both falsifications
are deterministic, both stand, and neither cost a measurement slot. A third,
the accidental-throttle reading, was withdrawn the same way by a
three-arm reproduction that separated the route channel from the rewrites.

The pattern that emerged is a discipline rather than a tool: state the
prediction in the design before the measurement, in a form that can be wrong,
and record the falsification in the section that made the claim. Several
sections of the design record end in retractions of their own earlier
paragraphs for that reason, and the report is better for it.

**The limits of that instrument are equally worth recording, because knowing
what the in-process harness cannot reach is what stops a false negative being
read as a clearance.** It reproduces the campaign's shape and direction but
not its magnitude. It reproduces the lane rule's suppression exactly, to a
write-to-defer ratio of 0.008 against the campaign's 0.01, and yet it never
wedges. What it does not model is enumerable and was enumerated: the
in-process peers carry no contract, there is one send sequence rather than
many, the receiver is a test sink rather than a Resident with a head slot and
Pack reassembly, and the relay is a channel rather than an Exchange. That
enumeration is the useful part: it says which instrument the next round needs,
which is a receiver-state export from the campaign side, not another twenty
repetitions of the same cell.

### 6.4 Score the substitution, not the component

Section 2.2 is the instance; the general form is worth stating. When two trees
pay for the same behaviour through different counters, scoring one counter
measures which tree pays through that counter, not which tree pays less. Look
for the substitution first, define the primary over both ends of it, and keep
the components as diagnostics.

### 6.5 Keep the negative result in the suite

Six tests in this suite exist only to assert that a rejected mechanism stays
rejected, and each names the finding that rejected it. That costs almost
nothing and it is the only mechanism anyone has for stopping a plausible idea
being re-proposed every six months by someone who was not in the room.

## 7. What is still pending

This report is not a completion notice. Four things are outstanding and one
correction is unfolded.

1. **The wedge classification fold-in.** The relay cell and the loss storm
   cell are being re-measured on an arm carrying the receive-queue drop
   counter, both lane-rule states, twenty repetitions each, so that every
   wedged run can be classified as the receiver deadlock or the proof-chain
   mode from its own counters rather than by inference. The runs in so far are
   unanimous for the proof-chain mode, every wedge reading zero drops with a
   write-to-defer ratio far below one, but the sample is partial and the
   counter did not exist when the original 160 scenario-runs were measured.
2. **A clean re-run of one contaminated block.** Two short in-process test runs
   overlapped three of the twenty repetitions of the forced-direct rule-off
   block, which breaks the host-idle rule this program measures under. The
   verdict does not turn on those three, but the block is being re-run and the
   verdict should be re-read against it.
3. **Full-suite verification of connect and server main.** Neither has been
   run. Connect main `b8f72dd` has had a build check and a reading of the
   affected tests but no test run; the server merge has not been made. Two
   tests carry comments predicting red on the tree they were written against,
   and whether either is red on the shipped tree is exactly what that run will
   say. One test, `TestP2pReadinessRequiresProbeQuality`, is skipped
   unconditionally, pending a probe-quality readiness gate that does not exist.
4. **The lane rule's own campaign.** The positional bound built to replace the
   unconditional re-arm is proven in process and unproven in a campaign. The
   bar is set: twenty repetitions per state on the relay queue-inflation cell
   and the two storm cells, judged on total recovery writes, dead windows, and
   a count of runs over 100 seconds that must be zero.
5. **The 65-repetition rerun** of the `exchange-h3 / 256k-down-64k-up` cell on
   a fresh seed set, which decides whether the hang of section 5.2 occurs at
   different rates on the two trees or at the same one.

And one item is owed to you directly rather than pending on us: **G1**, the
narrower overflow rule for the regression in section 3.1. It is specified, it
is unbuilt, and the regression it addresses is live on main.

## 8. Provenance

| Thing | Identity |
| --- | --- |
| The submitted work | urnetwork/connect#208 (transfer and p2p, 954 diff lines) and #209 (tun, 250 diff lines) |
| The merged tree used as control | connect `89e1633` with sdk `5f2652f`, which is the submitter's merged tree and is itself upstream on main |
| The shipped arm | connect `175d82a` with sdk `4fcce9f`; read from `DefaultSendBufferSettings` on connect main `b8f72dd` |
| The measurement ledger | `tests/PERFVAR-MEASUREMENTS.md`, campaigns `flightgate-mixed-20260910` through `flightgate-fixlane-20260913` |
| The calibration | `flightgate-aa-20260912` |
| The design record | `connect/FLIGHTGATEFIX.md`, 36 sections |
| The tests | `connect/flight_gate_*_test.go`, plus `net_http_plain_websocket_test.go` and three adapted files |

Campaign arms named `mergedX`, `a175`, `lane` and `fixlane` are detached
worktrees at their named commits plus a hand-applied working-tree patch adding
two relay-stall exports. The patch was verified behaviour-neutral hunk by
hunk in each arm; the verification is recorded in the ledger, because a later
reader cannot reconstruct it from the commit alone.

One phrasing in the design record is superseded and not yet corrected in
place: section 33.4 still says the relay stall is "three to four times the
timer's own ceiling". Section 5.1 above has the corrected figure. That file
carries another stream's work and the correction should be folded in by
whoever commits it next.
