# Multi client window management

This documents the landed behavior of `RemoteUserNatMultiClient` and its
window after the 2026-08 reliability checkpoint merge: upstream's
evidence/verdict system for exit health, the group/pinning placement work
(G-series), bench-time flow hand-off, and the two locally-found evidence
fixes reconciled into it (the lifetime connect-attempt clock and the
own-send-ack comparative proof). File references are to
`ip_remote_multi_client.go` unless noted.

The design center, stated once: **silence alone convicts nothing that is
carrying traffic; positive evidence acquits; destruction is budgeted,
observed, and proportionate.** Nearly every rule below exists because a
specific field incident (cited in the code comments) showed the naive
version destroying working connections.

## 1. The window

A `multiClientWindow` maintains a set of `multiClientChannel`s ("exits"),
each a dedicated `Client` to one provider destination. Window types:

- `WindowTypeQuality` (min 2 / target 6 / hard max 12): the default
  discovery window.
- `WindowTypeSpeed` (fixed size 1): single-exit; also the shape of a
  fixed-destination connection (an explicit client id, e.g. a user-selected
  network peer), which has **exactly one candidate and no substitute** — a
  property several rules below special-case.
- `WindowTypeAuto`: resolved from the performance profile.

`windowMinSatisfied` is what the UI renders as "connected". A warned sole
destination that is still routing keeps a fixed window satisfied — there is
no replacement to steer to, so warning it out only produces a permanent
"Connecting" display. A strict low-memory window at its ownership ceiling
likewise counts retained warned clients after confirming that at least one
ordinary client remains selectable. The strict admission gate cannot backfill
that warning without exceeding the memory ceiling, and excluding it from both
the minimum and admission counts creates an unwinnable state. A window with a
free replacement slot, or with every client warned, remains unsatisfied.

### Discovery and maintenance safety

Destinations come from the generator (`NextDestinations`, exclusion-aware:
`MultiClientGeneratorExcluder` keeps a removed provider from being handed
straight back — see `RemoveProvider`). Generator calls run through
`windowGeneratorCall`, which bounds each call with a deadline **without
letting the deadline own the result**: a late `NewClientArgs` result is
routed to cleanup (the platform identity is removed) and a late
`NextDestinations` is side-effect-safe to discard. This replaced the older
`MultiClientGeneratorContext` interface; the guarantee is the same —
a slow platform call can neither wedge the enumerator nor leak a client —
but it lives in the call wrapper now, not in a parallel interface.

### Admission

A candidate exit is admitted on an `IpPing` round trip (the transfer ack
proves the provider's receive path worked at that instant). Evaluation can
run a pool larger than the open slots (`EvaluationPoolMultiple`); losers are
cancelled with `ProviderStateNotAdded`. Every provider event
(`InEvaluation`, `Added`, `NotAdded`, `EvaluationFailed`, `Removed`)
carries the egress client id and the provider's location
(`monitor.AddProviderEvent`), so the UI's dots name real providers.

Admission proves *the transfer path worked once*. It does not prove the
return **data** path works — data rides contracts, transfer acks do not —
which is why the connect-evidence rules in §3 exist.

## 2. Flow placement: affinity, donors, pins

Flows (5-tuples) are pinned to exits in `sendUpdate` via
`multiClientChannelUpdate` records. Providers terminate TCP, so **moving a
live flow is breaking it**; placement is therefore sticky and the rules
below only ever apply to flows that do not yet have an exit.

- **Affinity groups**: flows to the same site (destination, server name,
  or cluster fallback groups) inherit the exit the group already uses
  (`inheritAffinityClient4WithLock`). The donor must be alive
  (`IsDone`-guarded), un-warned, and under the per-exit flow cap
  (`MaxFlowsPerExit`, default 16) — except `AffinityStickyPastCap` (default
  true) lets an established group's egress ip outrank the cap; the cap still
  gates every *new* site boarding the exit.
- **Retired donors**: a flow that just closed still donates its exit to the
  next flow to the same site. `update.Close` deliberately does **not**
  clear the committed client; the flow reaper later removes the record and
  nils the pointer atomically under the state lock, so the donor window is
  "from close until the reaper runs" and there is no observable
  registered-but-cleared state. This is what makes a browser's
  close-then-reopen land on the same exit. Pinned by
  `ip_remote_multi_client_donor_affinity_test.go`, including the IsDone and
  warned-donor guards.
- **Quarantined donors**: a benched donor inside the follow window
  (`QuarantineGroupFollow`, `GroupFollowWindow` 45s; the window is *bench
  age*, not receive recency) keeps its own site together
  (`donorQuarantineFollowed`); outside it the group scatters. The G-1
  ledger books one followed/scattered event per flow placement, aggregated
  across all the group lookups that placement makes.
- **Pins (G-4b)**: a site or app can be pinned to one exit
  (`flowPin`). The pin outranks the destination bridge, stays off the
  datapath and out of other groups, and gets `pinnedFollowWindow` (a
  bounded multiple of the ordinary follow window) — a pinned app cannot
  keep a failing exit both un-executed and un-released. Warned donors still
  refuse: a pin is not a license to board an unhealthy exit.

## 3. Health evidence

### Stats: windowed counters, lifetime clocks

Each channel keeps `packetStats` sampled into 1s event buckets over a 30s
window (`StatsWindowDuration`). **Windowed values age out**: bucket
eviction decrements the running counters, so any windowed quantity has a
hard ceiling of roughly `StatsWindowDuration + StatsWindowBucketDuration`.
Two liveness clocks are deliberately **lifetime**, not windowed:

- `firstUnansweredSendSynTime` — the start of the *current* unanswered
  connect attempt: armed by the first SYN when no attempt is pending, held
  through SYN retransmits (the device stack retransmits at 1s/2s/4s/8s; a
  "latest SYN" clock resets on every one and never matures), cleared by any
  received SYN. This exists because the windowed `firstSendSynTime` ages
  out at the very moment a 30s connect bar matures, which made the
  no-receive-syn verdict unable to fire for a genuinely silent route — the
  cold-start stall where a tunnel completes its TCP handshake and never
  delivers a byte. `unansweredConnectSince` prefers the marker with the
  windowed clock as an under-reporting fallback (skipped once a SYN was
  answered in the window, so an answered attempt is never resurrected).
- `lastSendAckTime` / `lastReceiveAckTime` — channel-level stamps used by
  the probes and the comparative proofs below.

### The three verdicts (`blackholeReasonFromStats`, pure)

- **`no-send-ack`** (hard): sends outstanding past `BlackholeTimeout` (5s)
  with zero send acks. The provider is not there.
- **`no-receive-ack`** (soft): sends acked but nothing returns for
  `BlackholeReceiveTimeout`, **and** the window's sends span at least
  `MinBlackholeDestinations` (2, load-scaled up by
  `BlackholeLoadCorroboration`: max(min, flows/8)) distinct destinations.
  One silent website through an acking exit is a dead website, not a dead
  exit. Note the receive bar has the windowed ceiling above — a value at or
  beyond ~31s silently never fires (`TestBlackholeReceiveTimeoutIsReachable`).
- **`no-receive-syn`** (soft): the current connect attempt unanswered past
  the connect bar with nothing received at all in the window. Reserved for
  an exit that has established nothing; connect trouble on a live exit is
  handled per-flow (dial-failure re-race) and by the dial-strike warning.

### Gates: inadmissible evidence (`blackholeGates`)

- `transportDown`: this channel's own transport set is empty — its silence
  proves nothing; every verdict is held.
- `uplinkStale`: tunnel-wide ingress silence past `UplinkStalenessGate` —
  the phone's own uplink is the prime suspect, so the receive-branch
  verdicts are held. `no-send-ack` deliberately is not: send acks ride the
  transport whose liveness `transportDown` tracks.
- `receiveFreshSince`: when a gated epoch ends (uplink recovers, transport
  re-registers), receive-branch clocks are **rebased** to the epoch end so
  held verdicts restart instead of all maturing at once on unfreeze.

Held verdicts are returned and counted, never silently swallowed.

### The comparative connect cut (`comparativeConnectTimeout`, pure)

The no-receive-syn bar is normally `BlackholeConnectTimeout` (30s) —
patient, because unanswered SYNs are ambiguous. The bar drops to
`BlackholeConnectComparativeTimeout` (10s) when positive evidence resolves
the ambiguity — the network works, so the fault is this exit's own. Two
independent proofs, either sufficient:

1. **Receiving siblings**: at least `comparativeReceivingSiblings` (2)
   *other* exits received return traffic within `BlackholeTimeout`. Two,
   not one — the claim is about the pool, and one lucky peer is not a
   statement about the pool. The sibling sweep only runs when the SYN age
   is inside the interval where the answer can change the outcome.
2. **The exit's own send acks** (`hasRecentSendAck` within
   `BlackholeTimeout`): a send ack is produced by the peer's receive
   sequence, so it proves the local path, the platform route, and the peer
   process are all alive — an unanswered SYN beside live acks is the peer
   failing to forward. This is the only proof a **single-destination
   window** (a network peer) can ever have; without it that first-class
   case always waited the full 30s to reclaim a dead route.

Both proofs still pass through the gates and sentencing — the cut only
moves *when* the branch matures.

## 4. Sentencing (`verdictAction`, pure)

The sentence is proportionate to blast radius:

- **Hard evidence executes** (`no-send-ack`): removal now, always.
- **Soft evidence against a flowless exit executes**: nothing to disrupt.
  Exception: an exit whose flows *we* migrated off at bench time
  (`emptiedByMigration`) is not treated as flowless-executable — the bench
  moved the only traffic that could produce the receive ack that acquits,
  so the exit serves its quarantine instead of being convicted for having
  been rescued.
- **Soft evidence against a loaded exit quarantines**
  (`SoftVerdictDemote`, default true): out of selection via warning,
  established flows keep running, removal deferred.
- **Expiry**: the same evidence held *continuously* since the quarantine
  began (any receive ack clears it, so an old quarantine start proves
  unbroken silence), past `StatsWindowKeepUnhealthyDuration` (60s), executes as
  `verdictActionExecuteExpired` — distinct in logs from an immediate
  execution.

### Quarantine lifecycle

- **Acquittal**: any receive progress lifts the quarantine.
- **Vacation** (`quarantineVacated`): a flowless quarantined exit with no
  firing *and no held* verdict is released — not acquitted. Its evidence
  aged out and nothing can re-convict or acquit it; without this it sat
  benched until rotation, a spare the window paid for and could not use.
- **Memory**: a survived quarantine leaves an `effectiveTier` demerit; the
  exit is only raced again when the healthier field is exhausted, until it
  earns promotion with a clean interval (`quarantineMemoryDuration`, 5m)
  and a proven connect.

### Bounded destruction

Removals draw from `RemovalBudgetCount` (2) per `RemovalBudgetWindow`, so
one bad pass cannot strip the window. `StandingReserve` and
`EffectiveTierSelection` shape refill and selection around the demerits.

## 5. Probes: convictions need a living witness

- **Busy probe**: a send stall (`SendStallTimeout` 3s of unacked sends)
  triggers an active liveness probe (`BusyProbe`, default on) instead of a
  passive conviction. The probe's ack is positive liveness
  (`addBusyProbeAck`) that resets the stall clock; a probe answer is a
  return packet that crossed the uplink, so it also counts toward the
  sibling proof.
- **Provider probe suite**: stage-B probes run through the exits
  themselves (the in-app suite pumps its own userspace tun, so probes take
  the same path as real traffic). Total probe silence across
  `ProbeSilenceWarnStreak` (2) passes **warns placement** — new flows steer
  away — rather than removing; any real receive acquits the streak
  (`probeSilentStreak`, with the acquittal CAS deliberately losing to newer
  silence).
- **Dial strikes**: several intercepted dial failures across multiple
  destinations with zero successes inside `dialStrikeWindow` (60s) mark the
  exit dial-starved (the resold-proxy-over-capacity signature) — again a
  placement warning, not a removal.

## 6. Retirement and hand-off

- **Retirement is a hand-off, not a deadline** (G-3): when an exit is
  benched, its *movable* flows (established quic-like udp/443 with inbound
  traffic, `rebindable`) are migrated at bench time rather than when
  execution lands. Migration must not feed the recovery tracker or the
  conviction evidence of the exit it rescued.
- **Flow records**: each `multiClientChannelUpdate` has a reaper that, once
  the update is done and idle, removes it from the path and affinity maps
  and nils the committed client — atomically under the parent state lock.
  (See §2 for the donor window this bounds.)
- **Channel teardown ordering** (`newMultiClientChannel`): essential
  cleanup first — deregister subscriptions, `RemoveClientWithArgs` (the
  platform identity; Pion close is slow and must not delay it), then
  `client.Cancel()` — and only then the observer-facing
  `client.CloseContractStats()`, which dispatches **synchronously** to
  stats listeners and can park behind a suspended app's observer. Ordering
  it first let a parked observer retain the platform identity and another
  client/transport record on every churn
  (`TestMultiClientCleanupPrecedesBlockedObservers`). The contract-close
  events still reach the still-attached listener — `CloseAllContractStats`
  is the deterministic synchronous backstop that emits regardless of the
  cancelled epoch worker — so a removed peer's contracts do not linger open
  in the contract-details UI.

## 7. Device-side reality

- `schedulerPauseDetected`: a host that slept between polls defers stale
  evidence for a recovery window instead of convicting on the gap.
- `DegradedLivenessScale`: low-power/thermal/constrained hosts stretch the
  probe and idle-ping cadences — a false removal (flow RSTs + reconnect
  churn) costs more than late detection.
- The uplink gate (§3) is the device-scale version of the same idea: when
  the whole tunnel is silent, suspect the phone before the pool.

## 8. Where the behavior is pinned

- `ip_remote_multi_client_blackhole_test.go` — verdict fixtures:
  reasons, gates, clock rebasing, comparative-cut bounds, receive-bar
  reachability.
- `ip_remote_multi_client_connect_evidence_test.go` — the lifetime attempt
  marker (arm / hold-through-retransmits / clear / re-arm), the own-ack
  comparative proof, and two end-to-end reclaims through the live
  detector (`TestMultiClientSilentRouteIsReclaimed`,
  `TestMultiClientLivePeerReclaimLatency` — reclaim at the comparative bar
  with SYN retransmits running).
- `ip_remote_multi_client_donor_affinity_test.go` — retired-donor
  inheritance and its IsDone/warned guards (and why `update.Close` must
  not clear the committed client).
- `ip_remote_multi_client_stall_test.go`,
  `ip_remote_multi_client_lifecycle_carry_test.go` — teardown ordering,
  rejected-send accounting, generator exclusion, plus the header notes on
  which pre-merge tests were superseded by this design and why.
- `multi_client_recovery_kernel_test.go`, rotation/migrate/network-peer
  suites — recovery, rotation, and hand-off machinery.

## 9. Provenance

The verdict/quarantine/probe/pinning system is the upstream reliability
checkpoint (merged 2026-08, connect `fb87085`). The lifetime connect-attempt
clock, the own-send-ack comparative proof, the teardown ordering, and the
donor-affinity/spec tests are the local stability program reconciled into
it (`f4d5acc`, `15b13bf`). Each rule's motivating incident is cited at its
definition; when changing a bound here, read the comment at the constant
first — the value is usually the residue of a specific outage.
