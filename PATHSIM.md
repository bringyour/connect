# The path simulator

A deterministic, virtual-time simulation of the relay path for the transfer layer, and the scenarios that pin the
findings of THROUGHPUT-RIG-REVIEW.md on it. Files: `pathsim_test.go` (the simulator), `pathsim_scenarios_test.go`
(S1–S5, S7, S8), `pathsim_inner_tcp_test.go` (S6).

## What it models

Two real `Client`s — a sender with its `SendSequence`s and a receiver with its `ReceiveSequence`s — joined by a
carrier of one or more hops, inside a `testing/synctest` bubble. Everything in the transfer layer is the production
code: the send window and the delivery-sized sizing rule, the resend queue and every recovery kind, the receive hold
and its policies, acknowledgement compression and the gap wake, the round-trip estimator, logical lanes, the shared
resend pools. Encryption is off.

Everything below the route channel is a model. Each hop direction is:

    ingress -> bounded queue (messages; drop on full, or block the writer) -> serialiser (bytes/s) -> delay (+jitter) -> egress

The queue with drop-on-full is the relay's non-blocking forward queue (`resident.go`, 4096 messages, `ForwardTimeout`
0); with block it is a socket buffer. The serialiser is the link rate or the relay's per-message work; the delay is
the wire. Loss is drawn per message at the serialiser, iid or in bursts, from a PCG seeded per link from the run seed;
a scripted list drops exact offered messages. Jitter may reorder or only spread arrivals. Hops chain, so a
client–relay–relay–provider path is three hops with their own queues and losses. The routes are published as H1 with
a reliable receive lane, exactly as the platform transport publishes its own, so the receive pump applies the
reliable-carrier handoff contract rather than dropping packs it has read.

Per run it records: goodput over the offer and over its second half (past any ramp), sender writes and resends with
their recovery kinds (timeout, selective gap, ack-tail and cumulative probes, deferred, eviction), receiver-side
duplicates (exact: data packs delivered by the last hop less packs admitted, after a drain in which everything
admitted arrives), hop drops (queue, loss, scripted) per direction, receive-hold drops, evictions and tentative
evictions, the longest gap between deliveries and the total head-of-line time (gaps above twice the round trip plus
two compression intervals), a stall flag, and the sender's window estimate.

## What it does not model

- No kernel TCP on either end, no provider NAT, no origin: the load is packs of 16 KiB payload offered as fast as
  the sender admits them. Findings whose mechanism sits in the client's kernel TCP (its reaction to delay and
  reordering, its receive-window collapse) or in the provider's TCP reader do not appear here, and the scenario
  comments say so where the rig disagrees.
- No websocket framing, no relay CPU, no memory pressure on the client. The relay is its queue and a rate.
- No CPU time at all. In virtual time a frame costs nothing to build, so the simulator's own ceiling is the transfer
  layer's clocking — one window per acknowledgement compression interval — measured once per process by
  `pathsimCeiling` (a 16 MiB window at zero delay: ~13 Gb/s) and every arm asserts its rate is under 70% of it, the
  instrument rule of `transfer_throughput_chain_test.go`. Scenario rates are bounded by the link rates the scenarios
  set (1 Gb/s), never by the host.
- The offered load never fills the window. A source that blocks on admission, the natural shape, races the sequence
  goroutine at every acknowledgement: the sequence clears its capacity flag and drains its pre-write queue while the
  caller's `awaitResendCapacity` fast path reads the flag, and which runs first decides how many packs leave in that
  instant and how many wait for the caller's 2 ms capacity poll. That was the one source of run-to-run difference
  found in the transfer layer, so the source does the sender's admission arithmetic itself (`pathSourceSet`): per
  lane, items times a conservative encoded size stay under the live window estimate; over all lanes, items above
  each lane's floor stay under the shared pool. One flow runs on lane zero; N flows run on lanes 1..N with equal
  floors of the constant window, sharing the product's one-window lane pool (which is why eight flows carry the
  same total window as one, as on the rig at 100 ms).

## Determinism

Virtual time fixes the order of events at distinct instants; the rest is arranged so that nothing that matters
happens at one instant on two goroutines:

- Every delivery carries a sub-microsecond seeded jitter, and deliveries on one in-order link are strictly
  increasing, so a link never hands a client two messages in one instant (two acks in one instant are coalesced
  into one snapshot the sender applies in map order).
- The source pushes one microsecond after a release, once the sequence goroutine has stored its capacity flag;
  lanes start staggered by 100 µs; the clients settle 5 ms before the offer so their client-key publications are
  alone at the origin.
- Each arm runs on one P (`GOMAXPROCS(1)`), so a goroutine readied by another runs after the readying one blocks:
  the receiver's ack worker, woken by the gap wake, then snapshots the head after the sequence goroutine has
  finished advancing it rather than in the middle.
- Link statistics freeze when the measurement ends, before the clients close, since a client's closing writes race
  the carrier's cancellation.

Each scenario prints a `digest` line hashing its integer results; three runs of the fast tier in separate
processes print identical digests (checked when this was written). A digest that moves between two runs of the
same tree is a new race, not noise.

## Running

    go test -run TestPathsim -v .                          # fast tier, a few wall seconds
    CONNECT_PATHSIM_FULL=1 go test -run TestPathsim -v .   # long offers and the whole S7 grid

`-v` is needed to see the tables (`go test` buffers a passing package's output). Each scenario logs one table: arm,
goodput over the offer and steady (second half) in Mb/s, writes, resends, timeout resends (`rto`), selective-gap
resends (`gap`), probes, duplicates, hop drops, longest gap, head-of-line time, drain time, the window, and flags
(`STALLED`, `UNDRAINED`, `sized`, `evict=`, `tentative=`, `rdrop=`, `HANDOFF=` and `DEADLINE=` — the last two are
instrument faults and must not appear).

## Adding a scenario

1. Build the path from `pathRelayHop(name, roundTrip, bytesPerSecond, queueMessages, loss)` or `pathHop` literals;
   set `Jitter`/`Reorder`, `BurstLoss`/`BurstLength`, `DropOffered`, or `Trace` on a `pathLink` as needed.
2. Build arms with `pathScenarioArm(name, hops, lanes, offer, sizing, configure)`; `configure` edits the two
   `ClientSettings` (window, hold, policies) before the clients are built; `sizing` and `pathReferenceBudget` are the
   process-wide surfaces the clients are constructed under.
3. Run each with `runPathArm`, then `reportPathScenario(t, name, results)`, which prints the table and the digest
   and refuses any arm inside the ceiling's censor margin.
4. Assert orderings or ratios between arms (`pathRatio`, `steadyGoodput`, `forwardDrops`, `resendCount`,
   `holBlocked`, `maxGap`, `drained`, `receiverEvictions`), never an absolute rate. Record the produced values in the
   test's comment, and where the rig disagrees say so rather than bending the assertion.
5. Gate long grids on `pathsimFull()` and use `pathOffer(fast, full)` for the offer length.

## Scenarios and findings

| Scenario | Finding (THROUGHPUT-RIG-REVIEW) | Reproduces | Notes |
|---|---|---|---|
| S1 `TestPathsimS1LossCostOnAShortPath` | §2: loss costs throughput on the short path | ordering yes, magnitude no | 0.5% costs 2.3% here (one gap resend per loss, no head-of-line time) against the rig's 33% before the receiver fixes; 2% costs 55% |
| S2 `TestPathsimS2ReceiverGapWake` | §2: gap wake and sorted selective acks | wake yes | +11% at one flow, head-of-line time 49 ms -> 0; the sorted write has no seam and is pinned by its unit tests |
| S3 `TestPathsimS3WindowRuleRegimes` | §1: the rule loses on the short path, gains at 100 ms for one flow, loses for eight | long/1 yes; short/1 as a ramp only; short/8 and long/8 no | short one flow 0.48x over a 2 s offer and 0.87x over 8 s but equal at steady state (the rig lost at steady state); 3.96x long one flow; equal at short eight; 4.12x at long eight where the rig's budgeted client read 0.62x |
| S4 `TestPathsimS4RelayQueueOverflowVersusWindow` | §1: a larger window into the relay queue costs drops, not throughput | yes | the dropped and resent shares of the sender's writes rise 2 -> 4 -> 8 MiB on both flow counts, and the counts too at eight lanes, where the offered load is the same at every window; one flow at 4 MiB now keeps 0.94 of its rate, every drop costing exactly one gap resend under the receiver's budgeted wake (0.18 before it, the rig 0.43), and collapses at 8 MiB (0.14); the eight-lane 8 MiB arm carries a 9 s timeout-path tail, explained in the test |
| S5 `TestPathsimS5SilentReneging` | REPORT §3.11c: the evicting receiver withdraws acknowledgements | withdrawal yes, 60 s stall no | committed-prefix withdraws none, the old policy withdraws 101 and is re-fetched by ack-tail probes; both collapse under the overrun and neither drain is asserted faster |
| S6 `TestPathsimS6InnerSegmentLossIsNotRetransmittedByTheProvider` | §6: the provider's TCP does not retransmit, so a post-delivery loss is a permanent hole | the provider's half yes | needs a client stack under synctest and a post-delivery drop seam for the whole; neither exists yet |
| S7 `TestPathsimS7HeavyLatencyGrid` | an instrument, not a finding | — | 50–300 ms one way x loss x rule; monotonic in delay without loss, rule above constant everywhere, loss costs everywhere |
| S8 `TestPathsimS8MultiHop` | an instrument, not a finding | — | two and three hops with queues; no stall without loss, bounded recovery with loss |

Provider standby release and upstream group merge have deterministic unit tests on other branches and are not
repeated here.
