# Throughput program: root causes and deterministic test coverage

An inventory of every defect or mechanism the throughput program established as
causing a behaviour, against what pins it today. Written so a builder can take
one row and go.

Derived from `THROUGHPUTFIX.md` sections 9 to 47, `THROUGHPUT-REPORT.md` on
main, and the tree at `3b2a188`, plus one uncommitted change noted in 0.1. Four
rows were fixed and pinned by other streams while this was being written; they
are marked RESOLVED in place rather than renumbered, so the identifiers stay
citable.

## How to read a row

Each row is one mechanism. Rows are numbered by class and the numbers are
stable; cite them in commits.

**Class.** `UNCOVERED` nothing pins it. `REMEDIATE` a row pins the right
mechanism but establishes it by sleeping, by elapsed wall-clock, or by racing a
timeout. `PARTIAL` a row touches the area but does not pin the mechanism.
`COVERED` a row asserts the contract in process and would fail if the defect
returned.

**Type.** `INVARIANT` a relationship among constants or derived values,
computable without running anything: no seam, no timing, cheap, and it keeps
failing years later when someone moves a constant. `DRIVEN` the defect exists
only in the sequence and must be run with time or ordering injected. `BOTH`
admits an invariant row for the arithmetic and a driven row for the sequence;
those are worth doing twice.

**Seam.** `-` none needed. `E` a seam already exists. `N` needs a new seam in
production code.

**The standing rule.** A test specifies the correct mechanism as a contract
rather than passing on the current tree, and where the pre-fix tree trades
against that behaviour the row is shown failing on main.

**One package-wide fact.** There is no general clock seam in `connect`. The
only injectable clock on a data path is `h1SaturationNowForTest`
(`transfer.go:11955`). `monotonicNanos()` (`ip.go:6847`) is a plain function,
and the whole acknowledgement-cadence path reads it, which is why every row in
`ip_tcp_ack_starvation_test.go` races real timers. The transfer side needs no
new seam: `sendWindowEstimate(now)`, `reliableAdmissionByteLimit(now)`,
`scheduleRetiredReliableCarrierRecovery(currentTime)` and
`rttWindow.estimate(sampleTime)` all take time already.

---

# 0. Rows that gate everything else, and where they stand

## 0.1 Two instruments the program diagnosed, one repaired and one in flight

Section 23 examined the race suite's one failure per run and concluded "the
tests, not the shipped code". It named both faults and both fixes. Both are now
addressed - one landed, one uncommitted in the working tree - which is why a
full run's red/green signal is becoming trustworthy again. Neither needs a
builder.

**B-01. RESOLVED while this file was being written.**
`TestWebRtcFastPathFitsIpv6MinimumMtuOnActualWire` asserted a fixed ten-second
deadline on ICE, DTLS and SCTP establishment - a step it does not test - so
under load it failed in setup before the behaviour under test was reached. It
now derives its bound from the runner's own deadline
(`transport_p2p_webrtc_loss_test.go:120-131`): `t.Deadline()` less a five-second
margin, floored at one second, with a message that says a failure there is
establishment rather than the property under test. That is section 23's remedy
as named. No action.

**B-02. `TestReceiverBudgetDropsDoNotWedgeEitherArm`** -
`flight_gate_receiver_budget_test.go:24`. It induced the receiver-budget
overflow it needs by sending faster than the receiver drains, so the
precondition was scheduling luck under load, which CODESTYLE's test rule
forbids.

**Status: being repaired, uncommitted, by another stream. Do not duplicate.**
The working tree adds `holdAfterFastDrop` to the mixed-lane harness: once the
hole at frame 25 exists, the next N data frames are withheld where they would
enter the receiver's inbound route and released together, so the queue is
carried past its cap by a counted event rather than by one goroutine outpacing
another. That is section 23's remedy as named - counted, not timed.

## 0.2 The row that was permanently red, and the repair that landed

`TestTheHandoffWaitIsTheSameForEveryTransport`
(`transfer_ack_handoff_transport_test.go`) asserted that the pack handoff waits
the same on a reliable and an unreliable carrier. The tree ships
`ReliablePackHandoffTimeout: -1` (`transfer.go:1007`) against `0` for unreliable
(`transfer.go:11048`), so the row failed by construction, and its commit said
so: "It fails on the tree as it stands, which is the point of it."

That is worse than no row. `.github/workflows/test.yml:39` runs the whole
package on every push with no skip, so the job is red regardless of what else
breaks, and one permanently failing row masks every genuine failure in a full
run.

**Status: LANDED.** The row is now
`TestTheHandoffWaitAsymmetryIsWhatItIs` (`transfer_ack_handoff_transport_test.go:39`).
It keeps the transport axis asserted uniform and pins the reliability axis at
the values the tree ships, with each failure message saying whether a change was
the fix or a drift, and it logs that it should be retired if the asymmetry ever
disappears. The suite is no longer red by design, so a full run's red/green
signal now means something again.

The underlying defect is unfixed and stays in this file as U-29. Section 38.14's
remedy remains: give the general path the wait H1 already has, and size
`AckBufferSize` (`defaultTransferBufferSize`, 32, `transfer.go:62`) to the
compressed acknowledgement burst rather than to the pack buffer. When that
lands, the characterisation row retires - it says so itself.

---

# 1. Findings that postdate the audit

## 1.1 The equilibrium identity is contradicted by the tree, and the row does not discriminate

The design states, as the fixed point's own identity, that occupancy rests at
about half the window. Measured pairwise at each tick the fixture reads 0.68 to
0.74. The explanation offered is that the half assumes the window is recomputed
from a delivery rate measured over exactly one round trip, while the estimator
recomputes from a ring spanning several, so the rested occupancy sits above the
idealised value.

`TestAtEquilibriumOccupancyIsHalfTheWindow`
(`transfer_window_properties_test.go:206`) now forms the ratio correctly and
asserts a 0.45 to 0.85 band. That band admits both 0.5 and 0.68, so it cannot
fail on the disagreement. The reconciliation is unpinned. See P-01.

Two adjacent inconsistencies: the function is still named `...IsHalfTheWindow`
while asserting two thirds, and the comment above
`TestGrowthDoublesFromAFullWindowAndStallsFromAHalfFilledOne` still explains
"why the unclamped equilibrium rests at half".

## 1.2 The ceiling chain is inert on the hardware that ships

The device memory target is 20 MiB on desktop and 24 MiB on mobile. Both sit
below the 64 MiB reference that `memoryTargetScale` (`memory_budget.go:79-84`)
divides by, which is the region where the old scaled form shrinks below its
constant and where a campaign would therefore read a null and conclude the work
was worthless.

State the arithmetic rather than the slogan, because the two forms are not
identical on the shipped targets. `tunBudgetShareByteCount` is
`max(512 KiB, budget/8)` (`tun.go:106`): at a 20 MiB target that is 2.5 MiB
against the old `MemoryScaledByteCount(mib(4), kib(512))` of 1.25 MiB, so the
draw is twice the constant there, not equal to it. The tree's own comment scopes
the identity precisely - "bit-identical to today's value at every budget where
the floor binds" - and at 20 and 24 MiB the floor does not bind.

So the landing is **not** inert for the two layers that have now moved. It is
inert for every layer still computed as `MemoryScaledByteCount` of a constant,
and that is what U-01 names. The instrument for this question is therefore
**U-01 and U-09**, both still open, plus the four rows that landed while this
file was being written (C-31 to C-34), which should be run against the 20 and
24 MiB targets specifically rather than only at the reference.

## 1.3 The 50 Mb/s fixture wall is an instrument, not the tree

`TestTheFixturePayloadCeiling` (`transfer_fixture_ceiling_test.go`) establishes
that the in-process fixture's throughput scales with payload size (63, 258 and
509 Mb/s at 1, 4 and 16 KiB) while the frame rate stays pinned near 7,800 a
second, so the binder is per-frame plumbing - the goroutine the delay pump
spawns per frame - not copy or serialisation bandwidth. That retires a standing
unknown and invalidates the "about 50 Mb/s whatever the window" framing.

It carries no assertion at all - one `t.Logf`, no `t.Errorf` - so it reports and
does not guard. See P-02: the precondition every window sweep depends on is
unpinned.

---

# 2. UNCOVERED, invariant - the parallel batch

Sixteen rows. No seam, no timing, no fixture. These can be written and run as
one batch.

**U-01 · Memory scaling runs in one direction only.** Every window and hold
outside the transfer share is `MemoryScaledByteCount` of a constant, and the
scale returns 1 at or above the 64 MiB reference and a fraction below, so all
were sized for a small host and can only shrink. Section 37.22.
`memory_budget.go:79-84,106`.
*Contract:* every sizing draw doubles when the process budget doubles, above the
reference as well as below, and no draw passes through `MemoryScaledByteCount`.
*Inputs:* `SetMemoryBudget` at 16, 64, 256, 1024 MiB.
*Refuted by:* any value flat above 64 MiB.
*On main:* fails identically.

**U-02 · RESOLVED, now C-31.** The H3 windows became a draw
(`h3BudgetShareDivisor`, `transport.go:762`;
`defaultH3MaxStreamReceiveWindowByteCount`, `:831`) and
`TestTheH3ReceiveWindowsAreADrawOnTheBudget`
(`transport_h3_window_share_test.go`) pins it. The ratchet at
`transport_h3_config_test.go:72` was rewritten against the helper rather than
deleted, which is the right disposal. Number retired, kept for citation.

**U-03 · RESOLVED, now C-32 and C-33.** The tun maxima became a draw
(`tunBudgetShareDivisor`, `tun.go:64`; `tunBudgetShareByteCount`, `:106`) with
an unbudgeted process keeping today's 4 MiB constant, pinned by
`TestTheTunsMaximaAreADrawOnTheBudget` and `TestTheTunStackResolvesTheDrawnMaximum`
(`tun_budget_draw_test.go`). The ratchet at `memory_budget_test.go:104` was
rewritten to assert the draw. Number retired, kept for citation.

**U-04 · RESOLVED, now C-34.** `TestTheBudgetFloorsFitTheSmallestSupportedHost`
(`tun_budget_draw_test.go:234`) sums the heap-backed commitments at the 8 MiB
minimum and fails if they exceed it, with a message saying a failure is a
finding to act on in the table rather than an assertion to loosen. That is the
row this entry specified, written as specified. Number retired, kept for
citation.

**U-05 · The share table's ratios are unbuilt.** Equal M/8 draws give the
carrier twice its proportional need and the transfer and tun rows half theirs.
Section 44.3. `transfer.go:711` is the only divisor that exists.
*Contract:* the H3, transfer and tun draws stand in the ratio their loops'
round trips imply, within tolerance.
*Refuted by:* three equal draws.

**U-06 · The unreliable carrier's flight ceiling is a constant pair.** 8 KiB to
256 KiB, which encodes 84 Mb/s at 25 ms - the tightest ceiling in the chain, and
the section 37.2 enumeration missed it. Section 37.14. `transfer.go:734-736`.
*Contract:* the maximum is a draw on the budget.
*Refuted by:* 256 KiB at every budget.

**U-07 · The server's H3 listener sets no flow-control windows.** Every accepted
connection runs the library's 6 MiB default, which binds both hop-directions
terminating there at about 218 Mb/s. Section 46.2.
`server/connect/transport.go:562`. Other repository.
*Contract:* `newConnectQuicConfig` returns a config whose
`MaxStreamReceiveWindow` equals the configured setting.
*Refuted by:* zero, which quic-go reads as its default.

**U-08 · Neither end requests carrier socket buffers.** Accepted sockets
autotune to the host's `tcp_rmem`/`tcp_wmem`, about 175 Mb/s of goodput at
200 ms on a stock kernel. Section 46.7. `net.go:282`, server `http.go:395-460`.
*Contract:* the listener and the dialer attach a buffer request derived from the
budget.
*Refuted by:* a bare `net.ListenConfig{}`.

**U-09 · The hold falls below the peer's window under 51.2 MiB of client
budget.** A provider runs unbudgeted at a 2 MiB window; a client at budget B
holds `max(320 KiB, 2.5 MiB x B/64 MiB)`. Section 37.17. `transfer.go:918`
against `:1025`, `memory_budget.go:21`, `ip.go:416-420`.
*Contract:* for every supported client budget, the receive hold is at least an
unbudgeted peer's send window.
*Refuted by:* B = 24 MiB, giving 960 KiB against 2 MiB.
*On main:* fails identically, with no harness at all. This is the cheapest
possible reproduction of the stall the program spent a campaign measuring.

**U-10 · The round-trip sample ring is fixed at 128.** `Rtt.Mean` is the mean of
the last 128 releases, not of the population, and the count reads 128 at every
path length; it produced a wrong thirty-per-cent inference. Section 38.15.
`transfer.go:871`.
*Contract:* the estimate reports that it is a ring, or exposes truncation.
*Refuted by:* `SampleCount == RttWindowSize` while writes differ fivefold.
*Status: PARTLY PINNED.*
`TestTheRoundTripEstimateIsARingOccupancyAndNotAPopulation`
(`transfer_rtt_ring_test.go`) writes five times the ring and asserts that
`SampleCount` saturates at the shipped `RttWindowSize`, and that `Mean` and
`Min` are defined over what the ring holds rather than over what was written.
The literal contract - that the estimate expose its own truncation - needs a
production field and is not asserted; the row says so and says it should be
extended rather than retired when one lands.

**U-11 · `SendShardCount` defaults to 1 and has no counter.** The single
goroutine hashing every packet from the device to its flow is the one truly
unsharded point below Transfer. Sections 31.9 and 19.3. `ip.go:506`.
*Contract:* the dispatch exposes a queue or wait counter.
*Refuted by:* no counter.

**U-12 · A nonzero lane count with a zero floor ships the starvation.**
`LaneFloorByteCount` defaults to 0 (`transfer.go:929`) while
`sdk/mobile_memory_policy.go:323` sets `LogicalDataLaneCount = 8` on mobile H1
and sets no floor. Section 28.3 states the rule; nothing enforces it.
*Contract:* a nonzero `LogicalDataLaneCount` implies a nonzero
`LaneFloorByteCount`.
*Refuted by:* the shipped mobile H1 settings.
*On main:* cannot fail - `LaneFloorByteCount` does not exist there. Branch-only,
which is why it needs a row.
*Status: PINNED for this module, and one finding on top.*
`TestALaneCountWithoutALaneFloorIsNotAConfigurationWeShip`
(`transfer_lane_floor_pairing_test.go`) holds the pairing over every settings
value `connect` ships and over the second half nobody wrote down, that the
floors must fit the pool they are exempted from. It cannot read
`sdk/mobile_memory_policy.go` - the sdk is a separate module that imports this
one - so the shipped violation still needs the mirror of this row in the sdk's
own package, and the row says so.
The finding: `TestTheCandidateLaneFloorDoesNotFitTheMobilePool` records that
the candidate scale the field's own doc names, `ResendQueueMinByteCount`, is
flat at 256 KiB while the pool it would be carved from,
`ResendQueueMaxByteCount`, is memory-scaled. Eight lanes commit exactly the
pool at the 64 MiB reference and more than the pool at every budget below it -
2 MiB against 768 KiB at the 24 MiB mobile target, which is the target where
`LogicalDataLaneCount = 8` is actually set. The floor must become a draw on the
same quantity the pool is a draw on, or the lane count must fall, before §28.3
can be followed on a phone.

**U-13 · A batch return's `true` reads as ownership transfer and means
delivered.** Section 29.3, `CODESTYLE.md`.
*Contract:* every entry point is declared borrows, takes, or takes-on-success,
and the batch entries' declaration matches their behaviour.
*Refuted by:* an entry with no heading.
*Status: PINNED for the declaration half.*
`TestEveryPoolBufferEntryPointDeclaresItsOwnership`
(`pool_ownership_heading_test.go`) reads CODESTYLE's list against the package
source with `go/parser` and fails on an entry with no word, on a rename out
from under the list, and on a "takes" confused with a "takes on success". It
failed on the tree as it stood: CODESTYLE's list had landed but eight of the
fourteen declarations it names carried no word, including both batch entries.
Those headings were written in the same commit, so the row is green.
The second half of the contract - that the declaration matches the behaviour -
is not decidable from source and stays with the pool boundary reconciliation.

**U-14 · An opaque host-supplied dial leaves the receive buffer unpinned.**
Where `DialContextSettings` is set the pre-connect hook cannot run, so only the
send pin is applied even where the rule says pin both. Section 15.2.
`upstream_socket_buffer.go:100-111`. Accepted, because a post-connect receive
pin is the generation-dependent freeze.
*Contract:* with an opaque dial and a policy that pins both, the send buffer
reads the pin and the receive buffer its default.
*Refuted by:* a pinned receive buffer after connect.
*Status: PINNED.* `TestAnOpaqueDialCannotReachTheReceivePin` and
`TestAHostSuppliedDialNeverRunsTheBufferControlHook`
(`upstream_socket_buffer_opaque_dial_test.go`) assert that the pre-connect hook
is built exactly when the policy pins anything - so a receive pin has one
application point and only one - that the shipped `TcpBufferSettings` wire it,
and that a `ConnectSettings` carrying a host-supplied dial never runs
`DialControl`. Written over the policy arithmetic rather than over syscalls, so
it runs on every platform; the kernel-level rows in
`ip_upstream_tcp_buffer_linux_test.go` are linux-only and only cover the
unknown-policy case.

**U-15 · `recovering` reads true through a quiet period.** The third exit is not
present as an exit; its effect is supplied by a fresh entry on the next arrival.
Accepted for behaviour, but any exporter of the phase state is misled.
Section 28.2.5.
*Contract:* the exported phase state is false during quiescence, or is
documented as not an exit.
*Refuted by:* a phase readout true after `AckCompressTimeout` of silence.

**U-16 · The platform's Reliability verdict reads identity, not reachability.**
`network_client.active` is an identity lifecycle an offline client keeps, so a
contract probe is granted for a dropped client and cannot detect death. This
rules out a whole class of simpler fix. Sections 10.2 and 14.1.
*Contract:* the verdict's input is reachability, or the caller does not treat it
as liveness.
*Refuted by:* a granted probe for a client with no transport.

## 2.1 The invariant rows that were declined, and why

Recorded so they are not re-attempted as written. Section 0.2's lesson governs:
a row that fails with no fix behind it is worse than no row, because
`.github/workflows/test.yml` runs the package on every push and one permanently
red row masks every genuine failure.

**U-05, U-06, U-08 · defect rows with no landing.** Each states a contract the
tree contradicts and that no branch is fixing. U-05's shape test wants the H3,
transfer and tun draws at 1 : 2 : 2 (§44.3's derived table puts H3 at M/16 and
the other two at M/8); the tree ships three equal M/8 divisors
(`transfer.go:711`, `tun.go:64`, `transport.go:762`), so the row is red on
arrival. U-06's `UnreliableMaximumFlightByteCount` is still the 256 KiB
constant (`transfer.go:890`). U-08's carrier dialer still attaches no buffer
request - `ConnectSettings.DialControl` is set only for the provider's upstream
socket (`DefaultTcpBufferSettings`), not for a carrier. All three land with
their fix, in the same commit, not before it.

**U-07, U-16 · other repository.** Both are server-side and cannot be written
in this module.

**U-11 · red as written.** The contract is that the dispatch expose a queue or
wait counter and no counter exists, so the row cannot pass until one does. The
adjacent invariant that IS available - `SendShardCount` defaults to 1 while the
udp/tcp user limits apply per shard, so raising it multiplies the effective
aggregate cap - is a different row from the one U-11 states and is left
unwritten rather than filed under this number.

**U-15 · not an invariant, and there is nothing exported to assert.**
`recovering` is a local variable in the acknowledgement loop
(`ip.go:5052`), not an exported phase state, so the row U-15 describes needs a
production exporter and a clock seam before it can be written at all.

**The provider budget cliff is already covered.** A positive process budget
switching a provider from unlimited flow counts and the 300 s
`providerUdpIdleTimeout` to scaled caps and the general 60 s reap is asserted
by `TestLocalUserNatSettingsMemoryScaled` (`ip_flow_limit_test.go:19`), in both
directions and including the ordering of the two idles. No new row.

---

# 3. UNCOVERED, driven and both

**U-17 · BOTH · N · The ladder's advertised window takes its path term from the
wrong loop.** It doubles and halves on the upstream writer's backpressure and
carries no round trip of the loop it advertises into - "the single most
inconsistent constant in the chain". Section 37.2 B4. `ip.go:462`, `:5815-5835`.
*Contract:* the advertised window derives from the loop it bounds.
*Refuted by:* a window that moves with upstream blocking alone.

**U-18 · RESOLVED, now C-35.** `SteadyAckEverySegments` exists on
`TcpBufferSettings`, and `ip_tcp_steady_ack_cadence_test.go` pins it with
`TestSteadyUploadIsAcknowledgedOnACadenceNotOnTheWindowHavingGrown` and
`TestSteadyCadenceDoesNotAcknowledgeShortOfItsSpacing`, both free of sleeps and
wall-clock reads. The nine section 26 rows now set it to zero explicitly so they
isolate the recovery phase from the cadence. Number retired, kept for
citation.

**U-19 · BOTH · - · The committed-prefix boundary arithmetic is asserted
nowhere.** Section 37.20. `transfer.go:13110-13160`.
*Contract:* an item commits if and only if
`missing_below(i) x maxHeldByteCount + sum(held through i) <= capacity`, and a
committed item is never removed.
*Inputs:* a receive sequence with a synthetic hold - delivery point, chosen
gaps, a set frame size. `commitHeldPrefix()` is callable directly and is pure
over queue state.
*Refuted by:* a committed item reaching the evict branch, or a commit whose gap
estimate exceeds capacity.
*Worth doing twice:* the driven outcome is already exercised by P-11.

**U-20 · BOTH · - · The binding-term diagnostic names the smallest window term,
not the throughput binder.** When a flow delivers at least half the target the
reason reads "the target" though throughput is set below it by something else;
25 of 98 measured arms read that way, and nobody read the column. Section 41.1.
`transfer.go:9884-9910`.
*Contract:* the reported term is the one that bound throughput, or the estimate
reports separately that the window was target-clamped and that throughput was
not window-bound.
*Refuted by:* "the target" on an arm whose delivery is below the target window.

**U-21 · BOTH · N · Android's physical-pressure input is never fed.** The
trimmer's physical arming compares `mobilePhysicalFootprintCurrent` against a
40 MiB threshold; only the Apple extension feeds it
(`sdk/memory_stats_ios_extension.go`, build tag `ios_extension`), Android's
`onTrimMemory` is commented out (`MainApplication.kt:393-397`) and `onLowMemory`
likewise (`MainService.kt:1149`), so the crossing never occurs and that loop
never reclaims at all. Section 38.10. `sdk/idle_memory.go:34,61`.
*Contract, writable today with no new seam:* on every platform the trimmer runs
on, the physical-pressure input has at least one non-test feeder.
*Refuted by:* an `android` build with no caller of
`recordMobilePhysicalFootprint`.
*Then:* the replacement sampler needs a new entry point
(`ReportMemoryTrimLevel`) and an `android`-tagged file. Do not mirror Apple's
threshold: section 38.10 shows that arms the trimmer permanently.

**U-22 · BOTH · - · A zombie holds its borrow from the provider's one shared
resend budget forever.** On an sdk-hosted provider every sequence shares one
budget (`sdk/device_local_provider.go:438`), so two or three dead clients
exhaust it and every live sequence is pinned at its 256 KiB floor - about
100 Mb/s at 20 ms. Section 13.3.
*Contract:* a released or dead destination's borrow returns to the pool.
*Refuted by:* `UsedByteCount` not falling after a release.

**U-23 · BOTH · - · The resend backoff advances only on one recovery path.**
`sendCount += 1` occurs only under `recoveryKind == sendRecoveryNone`
(`transfer.go:7875`), so a rewrite on a promoted-head or deferred-requeue path
leaves the interval pinned at its floor and a zombie emits at floor rate
indefinitely rather than decaying. Section 13.6.
*Contract:* every rewrite of an unacknowledged item advances the backoff.
*Refuted by:* a sequence of rewrites at a constant interval on a path other than
`sendRecoveryNone`.
*Note:* R-14 asserts the observable climb on the ordinary path only.

**U-24 · DRIVEN · - · The eviction notice silently drops past 4096 entries.**
Section 37.16. `transfer.go:9512-9519`.
*Contract:* an overflowed notice is reported, not dropped silently.
*Refuted by:* `receiveQueueEvictionNoticeOverflow` advancing with no other
signal.

**U-25 · DRIVEN · E · The acknowledgement-tail probe's interaction with
eviction is unpinned.** The probe re-sends the oldest selectively acknowledged
item without clearing the mark, at most twice, after which the sixty-second
lease applies - so an eviction costs a serialised probe interval per item, not a
flat minute. Section 37.19. `transfer.go:7255-7281`, `:884`.
*Contract:* an evicted item reaching the head is readmitted by at most
`AckTailProbeLimit` probes, and after that its resend time is send time plus
`SelectiveAckTimeout`.
*Refuted by:* a third probe, or a resend time below the lease after two.
*Seam:* `scheduleSelectiveAckRecovery(currentTime)` takes time already.
*On main:* possible - both constants exist there.

**U-26 · DRIVEN · - · The hold keeps what arrived first, not what is earliest
in sequence.** Under sustained reordering the kept set is arbitrary in order and
recovery lengthens by rounds. Section 37.18. `transfer.go:13032-13060`.
*Contract:* stated as the documented caveat, or the hold is ordered.
*Refuted by:* a drain shorter than the held run under reordering.

**U-27 · DRIVEN · - · There is no cumulative acknowledgement.** `receiveAck`
resolves one message id and releases that item, so every acknowledgement is
load-bearing and a lost one is recoverable only by resending at the timer.
Section 38.14.
*Contract:* everything at or below a sequence number is released by one
message.
*Refuted by:* a resend after a single dropped acknowledgement.

**U-28 · DRIVEN · E · A duplicate is re-acknowledged with an empty tag, so the
release is unsampled.** `observeAckRtt` skips `!tag.set`, so any item resent and
released by a duplicate's acknowledgement is invisible to the round-trip mean,
which then under-reports how long bytes are held. Section 38.14.
*Contract:* every release contributes a sample, or the estimate reports how many
did not.
*Refuted by:* `SampleCount` below the release count.
*On main:* the empty-tag path exists there.

**U-29 · DRIVEN · E · The acknowledgement handoff waits on one carrier class and
refuses on the other.** `packHandoffTimeout` returns `ReliablePackHandoffTimeout`
(-1, wait until capacity) for a reliable route and 0 for an unreliable one, so
the whole class of handoff-overflow faults is invisible on the path a cell would
naturally reach for. Section 38.14. `transfer.go:11035-11062`, defaults `:1007`.
*Contract:* the general path takes the wait H1 already has, and `AckBufferSize`
is sized to the compressed acknowledgement burst rather than to the pack buffer.
*Refuted by:* a compressed burst larger than `AckBufferSize` on a non-H1
transport producing a drop.
*Status:* see 0.2. The characterisation row in flight pins the trade; this is
the fix it retires against.

**U-30 · DRIVEN · E · A no-acknowledgement pack discarded after a failed carrier
write is counted and never asserted.** Section 38.7. `transfer.go:1609`,
`:2219`.
*Contract:* a failed write increments `SendNoAckDiscardCount`.
*Refuted by:* a discarded pack with the counter at zero.

**U-31 · DRIVEN · - · A client-side send refusal at slot admission is silent.**
`SendPacket` returns false and nothing counts it, where the provider's return
path counts its refusals. Section 38.7. `ip.go:9225-9236`.
*Contract:* a refusal is counted.
*Refuted by:* a refused packet with no counter movement.

**U-32 · DRIVEN · E · No-acknowledgement packs are converted to acknowledged
while a contract head is unacknowledged.** Measured at 23 per cent of a
contract's life at 200 ms and 47 at 400, and about 45 per cent over the first
200 MiB. Sections 38.7 and 39.1. `transfer.go:8596-8604`.
*Contract:* the contract is acknowledged ahead, so a no-acknowledgement pack is
never retained, sequence-numbered or resend-gated.
*Refuted by:* a no-ack pack entering the resend queue during a renewal.

**U-33 · DRIVEN · - · One sequence per client serialises every flow.**
`sendSequenceId` carries no flow or five-tuple, so a pack lost or delayed holds
every later pack of every flow of that client for one recovery interval, 300 ms
to 8 s - head-of-line blocking across unrelated TCP connections. Section 20.2.
*Contract:* with lanes enabled, a loss on one flow does not delay delivery of
another flow's later packs.
*Refuted by:* flow B stalling behind flow A's recovery at a nonzero lane count.
*Rows named and never built:* L1, L2.

**U-34 · DRIVEN · - · A UDP datagram over the read buffer is truncated
silently.** Each poller shard reads into 2,048 bytes and a short read discards
the rest of the datagram; EDNS permits 4,096. Section 12.1.
*Contract:* an oversized datagram is either read whole or reported.
*Refuted by:* a 4,000-byte origin datagram arriving as 2,048.

**U-35 · DRIVEN · E · The UDP kernel drop counter is unasserted, and reads zero
off Linux.** `ip.go:920-922`; `ip_udp_socket_drops_other.go:9` returns 0
unconditionally. Section 12.1.
*Contract:* on Linux the counter equals sent minus delivered; elsewhere it
reports unsupported.
*Refuted by:* a cross-platform assertion of "zero drops".
*Caution:* size the fixture by the skb charge, about 2,304 bytes for a
1,400-byte datagram, not by payload.

**U-36 · DRIVEN · - · The tun's outbound queue refuses and drops on a zero
timeout.** The link endpoint waits up to 250 ms for queue space, but the in-tree
delegations pass zero. Section 37.14. `tun.go:65,127-132`, `ip.go:9177,9236`.
*Contract:* the delegation passes the configured wait.
*Refuted by:* a zero timeout at the call site.

**U-37 · DRIVEN · E · Zombie resends inflate live flows' effective round trip
through the shared serial writer.** Not a fourth mechanism but the coupling that
carries the unexplained remainder. Section 35.
*Contract:* a dead destination's rewrites do not raise a live destination's
measured round trip beyond its own path.
*Refuted by:* `Rtt.Mean` on a live destination rising with the zombie count.

**U-38 · DRIVEN · E (stub writer) · The upload ladder collapses to its floor on
a backlog drain.** While the writer is stalled the counters do not advance; when
it resumes every parked byte completes as a blocking byte, and each halving
shortens the window the next halving needs, so a backlog of one window drives
the ladder from equilibrium to the 64 KiB floor in one drain. Recovery is about
seven round trips. Section 24. `ip.go:5815-5835`, floor `:431`.
*Contract:* with the writer held for one round trip and released, the window
descends to `MinWindowSize` within that drain, and regains equilibrium within
eight evaluations after it.
*Refuted by:* a descent that halts above the floor, or a climb needing more than
eight evaluations.
*Seam:* none beyond a stubbed writer - the ladder evaluates on offered bytes,
not on time, so this is fully deterministic.
*Rows named and never built:* `TestUploadWindowSettlesBetweenOneAndTwoBdps`,
`TestBacklogDrainCollapsesTheWindowToTheFloor`,
`TestCollapsedWindowClimbsInLogRoundTrips`. The highest-value unbuilt work in
the program.

**U-39 · DRIVEN · - · A backlog drain is charged as many windows of evidence.**
Bytes that block because a backlog is draining are counted as if the current
window exceeded the path, so one stall advertises 64 KiB for several round
trips. Section 24.
*Contract:* blocking during a drain does not count as window evidence.
*Refuted by:* more than one halving per drain.

**U-40 · DRIVEN · after fix · There is no per-destination liveness predicate.**
Return-path silence is read as death even while a direct P2P transport to that
peer is up; the route manager has only client-wide `HasActiveTransport()`.
Verified: `HasActiveDirectRoute` does not exist in the tree. Section 10.11.
*Contract:* silence is inadmissible while a direct route to that destination is
live.
*Refuted by:* a release fired while the peer's P2P transport is up.

**U-41 · DRIVEN · E · The abandon trade is unpinned.** A live client silent for
the timeout while the provider holds a carrier still has its NAT flows retired,
its sequences cancelled, its inner connections reset and its parked retained
bytes lost. Accepted, but nothing records it, and the per-provider release
counter named as a one-line follow-up is unbuilt. Section 10.11.
*Contract:* the release fires within [T, 1.3T] of the last acknowledgement, the
source is readmitted, and the event is counted.
*Row named and never built:* A11.

**U-42 · DRIVEN · - · A TCP return is admitted synchronously on the socket
reader's own goroutine.** The reader does not read again until admission
completes, so one flow's rate is capped at its 96 KiB read-ahead per admission
latency. Section 18 item 1.
*Contract:* the reader's progress is not gated on admission latency.
*Refuted by:* a single flow spending more than half its time in admission.

**U-43 · DRIVEN · - · Tuple-hashed lanes split a fragmented datagram across
lanes.** IPv4 fragments after the first carry no ports, so they hash elsewhere
than the first fragment and reassembly must tolerate arrival order.
Section 20.3.
*Contract:* a datagram fragmented across two lanes reassembles in either arrival
order.
*Refuted by:* a reassembler that assumes order.
*Row named and never built:* L3. A correctness precondition for any lane
rollout.

**U-44 · DRIVEN · - · Sequences race a broadcast where the managers use an
ordered grant.** Sequences wait on `CapacityNotify` and race `CanAdd`, while the
WebRTC managers use a FIFO grant list that scans past a large request so a
smaller one cannot starve; fairness among destinations above their floors is
that same broadcast race. Section 27.2.
*Contract:* releases are granted in waiting order and a re-registering heavy
borrower goes behind the others.
*Refuted by:* a starved small request.
*Row named and never built:* F4.

**U-45 · DRIVEN · - · E3 enters the start phase for most connections.**
`quiescentNanos` is stamped at sequence start, so a connection's first data
arriving later than the compression timeout enters the phase by E3 as well as
E2, and `StartQuickackByteCount` is not the only gate on start behaviour.
Section 28.2.6.
*Contract:* the campaign's start-window constant is the only gate, or the second
entry is documented and bounded.
*Refuted by:* start-phase acknowledgements on a connection below no byte gate.

**U-46 · DRIVEN · - · Latent: a pack reaching no terminal disposition would
wedge the silence clock.** `outstanding` would stay high and silence would key
on `lastAckNanos` alone, releasing an idle client once when it next parks.
Judged unreachable today. Section 10.4.
*Contract:* every queued pack reaches a terminal disposition.
*Refuted by:* a pack in neither state after its deadline.

---

# 4. REMEDIATE - right mechanism, wrong instrument

These pin the correct contract but establish it by sleeping, by elapsed
wall-clock, or by racing a timeout. They are not wrong; they are unreliable, and
this program has been bitten repeatedly by instruments that measured themselves.

## 4.1 The acknowledgement cadence rows - one seam converts nine

`ip_tcp_ack_starvation_test.go` carries eleven sleeps and fourteen wall-clock
reads because the path it drives has no clock seam: the compression wait arms a
real `time.Timer` from `monotonicNanos()` (`ip.go:5451-5490`), and the only hook,
`afterAckWaitWakeForTest` (`ip.go:5486`), reports that a wake happened, never
when.

**The single remediation:** add a per-sequence clock seam - a
`nowNanosForTest func() int64` on `TcpBufferSettings`, consulted wherever this
path reads `monotonicNanos()` - and a timer factory so the compression timer is
driven rather than awaited. Then each row below asserts counts against a driven
clock instead of against measured `elapsed`.

- **R-01 `TestAckCompressionIsTheOnlyClockBelowHalfAWindowRung`** `:37`. 600 ms
  `time.Since` loop with 5 ms sleeps; the allowance is `elapsed/AckCompressTimeout`.
  *Then:* acknowledgements over N advanced intervals exceed the timer-alone
  count of N. *On main:* fails by construction - the half-window rule and the
  timer exist there and the remedy does not. This is the calibration item whose
  failing demonstration on main is possible and unwritten.
- **R-02 `TestFirstSegmentAfterATimeoutIsAckedAtOnce`** `:160`. Races a 500 ms
  bound.
- **R-03 `TestBurstEndTriggerArmsOnFirstArrivalAndRearms`** `:206`. Sleeps a
  multiple of the bound.
- **R-04 `TestSteadyStateUploadEmitsNoQuickacks`** `:277`. Allowance derived
  from measured elapsed.
- **R-05 `TestCountingRuleCountsSegmentsNotBytes`** `:356`. Same.
- **R-06 `TestBurstEndWakesPerIntervalNotPerArrival`** `:430`.
  `allowedWakes = elapsed/quiescenceBound + 2`.
- **R-07 `TestConnectionStartQuickackIsBounded`** `:524`. 200 ms sleep so the
  last acknowledgement lands before counting.
- **R-08 `TestRetransmissionEntersTheRecoveryPhaseAndTheBoundEndsIt`** `:579`.
  20 ms and 200 ms sleeps around the count.
- **R-09 `TestOverdueBurstEndIsAckedAtOnce`** `:652`. Sleeps and races a bound.
  *Severity note:* section 28.5 retracted this defect's premise - the pure
  acknowledgement's admission cannot block - so it is latent, reachable only
  under a test hook. Keep as hygiene; do not schedule as a production fix.

## 4.2 The rest

- **R-10 `TestLiveClientStalledPastAbandonTimeoutIsNotRetired` and siblings**
  `ip_provider_silent_source_test.go:87,142,208,252,284`. Tickers at T/4 and
  T/2, and the assertions are `elapsed` in [T, 1.5T] - the bound *is* the
  contract. *Then:* drive the evidence clock and assert the decision against
  computed silence rather than measured elapsed. Seam: `returnAckTargetForTest`
  exists; the silence clock does not.
- **R-11 `TestTheEvictionNoticeStillServesAReceiverThatEvicts`**
  `transfer_receive_eviction_test.go:86`. 10 s deadline with a 50 ms poll.
- **R-12 `TestUdpFlowReleasedWhenClientDisappears`**
  `ip_udp_flow_reclaim_test.go:27`. Sleeps three idle timeouts per cycle; the
  goroutine and descriptor census is valid only if reaping finished.
- **R-13 `TestZombieFlowEgressIsBoundedByItsResendQueueAndInterval`**
  `transfer_zombie_egress_test.go:63`. 500 ms observation window.
- **R-14 `TestDeadDestinationResendIntervalGrows`** `:241`. 12 s sleep;
  intervals from real timestamps. Asserts the observable climb on the ordinary
  path only - see U-23 for the mechanism.
- **R-15 `TestWriteWaitEstimateSeparatesAnIdleWriterFromALoadedOne`**
  `transfer_write_wait_test.go:24`. A 40 ms sleep creates the very wait being
  measured.
- **R-16 `TestLightLaneDeliveryBesideASaturatingLane`**
  `transfer_lane_floor_test.go:433`. 2 s observation; the bar is bytes per real
  round trip against a measured band.
- **R-17 `TestALargerWindowIsFasterAtALongRoundTrip`**
  `transfer_send_window_test.go:993`. 6 s offers; the assertion is a measured
  rate ratio of at least 1.7. Its ceiling and advertisement assertions are
  exact and worth keeping.
- **R-18 `TestSizedWindowShrinksWhenThePathShrinks`** `:863`. 4 s and 6 s offers
  with a mid-flight rate change; the 3x bar is derived from four runs. See P-03.
- **R-19 · B-01 and B-02 of section 0.1**, which are REMEDIATE items in
  pre-existing rows and are listed there because they gate everything else.

---

# 5. PARTIAL - touches the area, does not pin the mechanism

**P-01 · The equilibrium identity is not reconciled.**
`TestAtEquilibriumOccupancyIsHalfTheWindow`
(`transfer_window_properties_test.go:206`) asserts `0.45 <= ratio <= 0.85`, a
band that admits both the design's 0.5 and the tree's 0.68 to 0.74, so it cannot
fail on the disagreement. Sections 38.15 and 40.2 still state the half as the
fixed point's identity.
*Contract:* the rested ratio approaches the design's half as the delivery ring's
span approaches one round trip, and sits above it otherwise.
*Inputs:* vary the ring span with the round trip fixed; compute the predicted
ratio from the span.
*Refuted by:* a ratio that does not move with the span, which would mean the
offered explanation is wrong and the identity is contradicted for some other
reason.
*Also:* rename the function, and correct the stale "rests at half" comment above
`TestGrowthDoublesFromAFullWindowAndStallsFromAHalfFilledOne`.

**P-02 · The fixture's frame-rate ceiling is reported, not asserted.**
`transfer_fixture_ceiling_test.go` carries one `t.Logf` and no assertion, yet
every window sweep in this program runs on that fixture and is only meaningful
below the ceiling.
*Contract:* a window cell's permitted rate is below the fixture's frame-rate
ceiling at the payload it uses.
*Refuted by:* a sweep whose permitted rate exceeds it, which reads inside the
null band for reasons unrelated to the tree.

**P-03 · The initial size is never a lower clamp.** R-18 asserts a 3x ratio from
a paced drain change, not the property. Section 37.13.
*Contract:* on the estimator fixture with samples present, a delivery rate below
`Initial` yields `Window < Initial`.
*Inputs:* `newEstimatorFixture`, `observeDeliveredBytes` at a low rate,
`sendWindowEstimate(now)`.
*Refuted by:* `Window == Initial` with `Sized` true.
*Seam:* exists. *On main:* not possible - no rule there.

**P-04 · Obtainable must never clamp the window.**
`TestManySequencesSizingAgainstOneBudget:59` asserts obtainable is *reported*
below the pool, never that it does not clamp. Section 38.9.
*Contract:* with `budget.Available()` at zero, the estimate still returns the
share as `Ceiling`.
*Refuted by:* `Ceiling == Floor`. Deterministic, no timing.

**P-05 · The eviction-notice cascade is logged, not asserted.**
`transfer_receive_eviction_test.go:86` records that a notice under an overrun
evicts a second item to readmit the first. Section 37.19.
*Contract:* with the advertisement in force there is no overrun and no
rotation.
*Refuted by:* a second eviction caused by a noticed resend.

**P-06 · Refusal truthfulness is unasserted.** `transfer_receive_no_evict_test.go`
carried it and was deleted by `95bd01b`; `ReceiveHoldRefuse` survives as a
shipped policy and a comparison arm. Section 37.20.
*Contract:* under refuse, a refused arrival produces no `sendAck` for its
sequence number.
*Refuted by:* any acknowledgement naming a refused sequence number.
*Note:* structurally true today - `transfer.go:13076` returns without
`sendAck` - which is exactly why it needs a row rather than an audit.

**P-07 · The bistable 104,448 receive window is characterised, not guarded.**
`TestUpstreamTcpReceiveWindowWithoutAutotuning:33` manufactures the state and
says so. Section 16.
*Contract:* the provider's own upstream socket is not in that state under load -
which is `TestUpstreamTcpReceiveBufferGrowsUnderLoad`, and that row is the
guard.

**P-08 · A pack's deadline past admission is unasserted.** Section 38.12.
*Contract:* a queued pack is bounded by an absolute deadline carried from entry,
not by the settings' write timeout.
*Refuted by:* a pack waiting past its caller's deadline.

**P-09 · The client IP layer's flow keying pins a fixture, not production.**
`TestTheSingleDestinationClientKeysItsIpTrafficPerFlow:39` - the row itself
records that this layer is a test fixture and that production goes through the
multi client. Section 38.8.

**P-10 · Transfer-layer acknowledgement compression is not asserted as a
round-trip term.** `ReceiveBufferSettings.AckCompressTimeout` is 10 ms and adds
a mean 5 ms to every acknowledgement's round trip - the largest divisor term we
own. Section 32.2.
*Contract:* the measured round trip contains the compression term.
*Refuted by:* a round trip insensitive to it.
*Row named and never built:* W3.

**P-11 · The hold policy asserts drainage, and the clock still frames the run.**
`transfer_receive_hold_policy_test.go:79` now asserts
`committed.delivered >= messageCount*9/10` with an explicit rationale - a real
improvement over demanding completion inside the cell's clock - but a 30 s
deadline with a 50 ms poll still bounds the run, and the committed arm's zero
acknowledged evictions remains the load-bearing assertion.
*Contract:* zero acknowledged evictions, and drainage strictly better than
refusal, both computed rather than raced.

**P-12 · Throughput scaling with the resend queue at fixed delay is unpinned.**
No row sweeps the queue as the independent variable. Section 32.1.
*Contract:* below the plateau, throughput scales with the queue and halves when
the round trip doubles.
*Refuted by:* flat arms.
*Rows named and never built:* W1, W2. Must respect P-02's ceiling.

---

# 6. COVERED

| # | Contract pinned | Row |
|---|---|---|
| C-01 | Transfer share is a draw on the budget, never a scaled constant | `TestTheTransferShareIsADrawOnTheBudget` `transfer_window_sizing_switch_test.go:221` |
| C-02 | Resolved ceiling moves with the budget | `TestTheResolvedCeilingMovesWithTheBudget:311` |
| C-03 | Ceiling reads a budget attached after apply | `TestTheCeilingReadsABudgetAttachedAfterApply:384` |
| C-04 | Window steps to advertised capacity, no ramp | `TestTheWindowStepsToTheAdvertisedCapacity` `transfer_window_properties_test.go:64` |
| C-05 | Delivery cap is one-sided and lagged, and names its reason | `TestTheDeliveryCapIsOneSidedAndLagged:109` |
| C-06 | Growth doubles from a full window, stalls from a half-filled one | `TestGrowthDoublesFromAFullWindowAndStallsFromAHalfFilledOne:263` |
| C-07 | Window computed from measured minimum round trip, not the 300 ms resend floor | `TestSizedWindowIsComputedFromTheMeasuredRoundTrip:770` |
| C-08 | The same interval defect in the reliable admission bound | `TestTheReliableAdmissionBoundReadsThePathNotTheResendFloor:460` |
| C-09 | The rule refuses to coexist with delivery-bounded admission | `TestTheRuleRefusesToCoexistWithDeliveryBoundedAdmission:260` |
| C-10 | Switch off is byte-for-byte today's behaviour, wire fields absent | `TestTheWindowSizingSwitchOffIsTodaysBehaviour:26` |
| C-11 | The window has exactly one owner | `TestTheWindowHasOneOwner` `transfer_window_single_owner_test.go:35` |
| C-12 | Advertised quantity is capacity, not free space | `TestTheAdvertisedWindowIsCapacityRatherThanFreeSpace:207` |
| C-13 | A sender clamped to advertised capacity forces no eviction | `TestAnAdvertisedCapacityRemovesTheEvictionEntirely:259` |
| C-14 | A carrier change voids selective acknowledgements its route earned | `TestACarrierChangeVoidsSelectiveAcknowledgements:58` |
| C-15 | The resend queue never holds an unacknowledged item | `TestTheResendQueueNeverHoldsAnUnacknowledgedItem:300` |
| C-16 | A no-ack pack behind a full resend queue is written, not refused | `TestANoAckPackIsNotHeldOrDroppedByAFullResendQueue:96` |
| C-17 | An unpublished capacity gate admits and costs no budget; a published one binds | `TestTheCapacityGateFailsOpenUntilTheLoopPublishes` `transfer_capacity_gate_fail_open_test.go:32` |
| C-18 | Acknowledgement compression stays under a quarter of the peer's retransmission floor | `TestAckCompressionStaysUnderTheRetransmissionFloor` `ip_tcp_ack_compression_floor_test.go:35` |
| C-19 | Pin only where it beats the kernel's autotuning ceiling | `TestUpstreamSocketBufferPolicyPinsOnlyAboveTheAutotuningCeiling:414` |
| C-20 | The mobile budget table and the kernel's clamp-and-double | `TestUpstreamBufferSizingUnderMobilePolicy:46` |
| C-21 | The rule reaches a real flow through the dial hook | `TestUpstreamFlowSocketFollowsTheBufferPolicy:26` |
| C-22 | UDP buffer sizing, linearity, skb charge, with a positive control on the drop counter | `TestUpstreamUdpBufferSizing:45` |
| C-23 | An undecided policy never pins | `ip_upstream_tcp_buffer_linux_test.go:32,57` |
| C-24 | A non-retained ack timeout closes a sequence holding retained items | `TestNonRetainedAckTimeoutClosesTheSequenceWithRetainedItems:30` |
| C-25 | A datagram return never enters the abandon retry | `TestDatagramReturnDoesNotEnterTheAbandonRetry:367` |
| C-26 | Pool ownership violations fail the test that caused them; leaks fail at teardown | `message_pool_ownership_test.go:175,203,226` and `TestMain` |
| C-27 | Unsampled is a fact, not a zero; the minimum travels with the mean | `TestRttEstimateCarriesItsMinimum:218` |
| C-28 | The stats path is safe to read while the sequence runs | `TestSendWindowStatsAreSafeToReadWhileTheSequenceRuns:924` |
| C-29 | Lane floors are exemptions, not reservations; a one-lane client pays nothing | `TestLaneFloorsAreExemptionsNotReservations:290`, `TestOneLaneClientPaysNoFloor:174` |
| C-30 | A clamped window is the ceiling and fills it, and follows its share down and up | `transfer_window_clamped_test.go:45,162` |
| C-31 | The H3 receive windows are a draw on the budget | `TestTheH3ReceiveWindowsAreADrawOnTheBudget` `transport_h3_window_share_test.go` |
| C-32 | The tun's maxima are a draw on the budget | `TestTheTunsMaximaAreADrawOnTheBudget` `tun_budget_draw_test.go` |
| C-33 | The gVisor stack resolves the drawn maximum | `TestTheTunStackResolvesTheDrawnMaximum` `tun_budget_draw_test.go` |
| C-34 | The heap-backed floors fit the smallest supported host | `TestTheBudgetFloorsFitTheSmallestSupportedHost` `tun_budget_draw_test.go:234` |
| C-35 | A steady upload is acknowledged on a cadence, not on the window having grown, and never short of its spacing | `ip_tcp_steady_ack_cadence_test.go` |

Notes. C-19 to C-23 are `//go:build linux` and do not run on a darwin
workstation; CI runs `ubuntu-latest`, so they do run there. C-18 sits at exactly
its boundary - 50 ms against `tcp.MinRTO`/4 of 50 ms - so any increase in the
compression timeout or decrease in the vendored floor fails it, which is its
purpose.

---

# 7. Counts

| Class | Count |
|---|---|
| UNCOVERED | 40 |
| REMEDIATE | 18 |
| PARTIAL | 12 |
| COVERED | 35 |
| **Total** | **105** |

By type, over the 70 rows that need work:

| Type | Count | Needs a new production seam |
|---|---|---|
| INVARIANT only | 13 | 0 |
| DRIVEN only | 43 | 4 |
| BOTH | 14 | 2 |

The shape of the work. Thirteen invariant rows need no seam, no timing and no
fixture, and they include the two highest-severity unpinned defects, U-01 and
U-09, which are also the instrument for 1.2. Eighteen more are existing rows
that pin the right mechanism and need only a clock seam, and a single seam on
the `ip.go` acknowledgement path converts nine of them at once. The genuinely
expensive set is small: four driven mechanisms needing new seams, plus the
Android sampler, which is a platform integration rather than a test.

Five rows moved from UNCOVERED to COVERED during the day this file was written -
U-02, U-03, U-04 and U-18, plus the two blocking instruments of 0.1 and 0.2, of
which one landed and one is in flight. Re-read the tree before starting a row;
the numbers here are stable but the tree is not.

---

# 8. Where the record and the tree disagree

Reported, not resolved.

1. Section 38.9 describes the ceiling faults as unfixed; `651c7c4` landed all
   three and C-03 pins them. Section 47.3 agrees with the code.
2. Section 36.4 cites `ip.go:421` for "the ladder's maximum is 1 MiB". That line
   is the UDP profile's `MaxWindowSize`; the TCP ladder's maximum is at
   `ip.go:462`. The two are conflated.
3. Section 47.7 lists the binding-term diagnostic as outstanding while `3e08bc2`
   narrowed `TargetBound` to cases where the target actually narrowed the
   ceiling. The defect stands in substance - see U-20 - and the wording no
   longer matches.
4. Sections 26.6 and 25 describe Q1, Q3, Q5 and K1 to K3 as the rows that pin
   the starvation. None exists. `TunTcpConn.TcpInfo()` (`tun.go:1543`) was built
   specifically to serve K3.
5. Section 37.16 says every resend path skips a marked item; section 37.19
   corrects it to a fourth path. The code has six read sites
   (`transfer.go:2601,2730,7036,7060,7217,7234`), so the corrected statement is
   the accurate one.
6. The equilibrium identity - see 1.1 and P-01.

Of 73 test names appearing in the record and the report, 42 resolve to nothing
in the tree; three are artefacts of the search and several are renames
(`TestUdpReturnNeverEntersTheAbandonPath` became
`TestDatagramReturnDoesNotEnterTheAbandonRetry`,
`TestLightLaneKeepsItsShare...` became `...KeepsItsFloor...`,
`TestLaneCostIsReadInRetainedBytes` became
`TestLaneFloorMemoryCostTracksItsDerivation`). The true absences are Q1, Q3, Q5,
K1, K2, W1, W2, W3, D2, D5, M2, M3, F4, L1, L2, L3, L5, L6, L7, R1 to R5, Z2, Z3
and section 24's three ladder rows.

**A reading hazard.** `THROUGHPUTFIX.md` is not in section-number order. The
physical order runs 9 to 18, then 16.4, 15.4, 10.11, 16.5, 18.1, 19 to 22, 20.4,
20.5, 23 to 27, 26.7 to 26.9, 13.6, 26.10, 28. The out-of-order subsections are
where most of the self-corrections live, so anyone reading it sequentially by
section number will read superseded claims as current.
