# Agent report 2026-09-15: upstream window rule review, rig verification, beta state, PRs #213/#214

Audience: agents continuing this work. Dense by design. Every number here has run-level detail in
`~/urnetwork-perf/LEDGER.md` (sections noted as Pnn). The human summary is the Claude artifact "Throughput review".
The earlier decision write-up is `~/urnetwork-perf/UPSTREAM-WINDOW-RULE-REVIEW.md`.

---

## 0. State at a glance

| Item | State |
|---|---|
| `beta/custom-server` (fork Ryanmello07/connect) | `1e34c6fc`. Option A: upstream/main `3a593216` merged, window rule default OFF, our gap-ack + standby fixes, `THROUGHPUT-RIG-REVIEW.md`. Pushed. |
| Upstream PR #213 | `pr/option-a-fixes-rule-off` @ `b54f9f72`. Commits on upstream/main `3a593216`: `dd976e15` standby release, `1317530a` gap acks, `92e0ac3c` rule default off, `b54f9f72` review doc. |
| Upstream PR #214 | `pr/aggressive-upstream-group-merge` @ `3bcd5aba`. #213 plus `3bcd5aba` group merge. Conflicts with #213 by construction; merge one or the other. |
| User decision | Beta runs A. Both PRs are open so the dev team can run their own tests. The user is sending their views to the dev team before any new work starts. **Do not start new implementation until the user says so.** |
| Rig provider | `urprovider.pa` md5 `99de16bbe8d5` (A build). Provider sysctls stock. Client host rmem/wmem stock. |
| Worktrees | `connect-pra` (PR A), `connect-prb` (PR B). `connect-merge` removed. Local branch `exp/upstream-sndbuf-uncommitted` parks the old SO_SNDBUF experiment (superseded by upstream's conditional pin). |

---

## 1. What upstream shipped (dev report "window rule shipped", connect `3c918bde` → `3a593216`, sn `cca9b43a`, sdk `cb1e590`)

**Shipped**
- **Delivery-sized transfer window rule, default ON** (`transfer.go` `init()` → `defaultWindowSizing.Store(WindowSizingFromDelivery)`).
  - Window = k × delivery over the round trip.
  - Clamped by a 1 Gb/s target and by the peer's advertised receive hold.
  - Rollback: `SetWindowSizing(WindowSizingConstant)`.
- **Receiver window advertisement** and committed-prefix / never-evict receive path.
- **A carrier change voids selective acks** (`4533701`).
- **Provider memory derived from host.**
  - Target = (4/5 host) / (3 × count); budget = soft limit.
  - Flow caps decoupled from the budget (connect `c5f058bf`, sn `3662003` + `cca9b43`).

**Their evidence**
- In-process fixture, one layer (transfer window + hold), 200/400 ms, 2.4–4.0× (§3.23).
- §4: "No cell has used a native operating-system stack."
- §8.3: below the 10 ms AckCompressTimeout the growth factor `g = 2 rtt_min / (rtt_min + c)` < 1, and the window walks to its floor ("short-path problem remains").

**Overlap with our findings**
- Their §3.11c silent reneging = our P21 mechanism 2 and the P43 stall route.
- Their §8 compression term `c` is what our gap-ack fix changes.
- Their fixture cannot see relay loss scaling with window (our P31).

---

## 2. Rig and harness (reuse these; do not reinvent)

**Hosts**
- US client and provider in one datacenter, network RTT ~0.3 ms.
- Germany client ~100 ms away.
- Shared beta relay, read-only; never restart it.
- SSH targets and IPs are not in this tree: the scripts in `rig/` read them from the environment (`rig/env.example`). Do not copy them into committed docs.

**Harness**
- `ceil2.sh LABEL PROVIDER_BIN FLOWS DUR TARGET CLIENT_ENV PROVIDER_ENV`
  - Restarts the provider (25 s settle) and runs the client via `tmeas.sh` (TUN) or `cmeas.sh` (socks) on the client host.
  - Prints goodput plus client/provider/relay CPU and the md5 of the executed binaries. Env: `CLIENT=`, `CLIENT_MODE=tun`, `TUN_BIN=`.
- `tdone.sh`: the same with TDIAG provider parsing.
- `upstat.py FILE PREFIX`: medians per arm.
- Matrix scripts: `upmatrix.sh`, `prmatrix.sh` (arms rotate per repetition); `rollback.sh`, `rollbackde.sh`.
- Stall catcher: `catch.sh` / `catch2.sh` / `catchpa.sh` with `stallwatch.sh` on both hosts.
  - Dumps goroutines (provider pprof :6060, client :6061), `ss -tinm` and `nstat` when the NIC rate drops below 50 Mb/s for 4 s after exceeding 300.
  - Mac-built providers have no pprof.
- `bldrop.sh`: per-run `nstat` deltas (backlog drops, prune, OFO).

**Rig client**
- Scratch `urw/proxy/tunclient` (not the product SDK).
- Relay-only switch via `tdiag/winpatch.py` (`URNETWORK_NO_P2P=1`).
- `HARNESS_MEMORY_BUDGET_MIB` calls `connect.SetMemoryBudget` before settings, to emulate a budgeted desktop. It is not an sdk device: attached pools differ.
- The TUN client writes packets one by one to `/dev/net/tun` (no GRO), so the kernel ACKs every ~2 segments.

**Build recipes**
- Provider on the VPS: `/opt/urNN` = copy of a sibling tree with `connect` replaced; `cd sn/cli/miner && GOFLAGS=-mod=mod go build`.
  - sn main needs the new sdk dependency (webtransport-go).
- Provider cross-built on the Mac: `scratchpad/provbuild` (sn, sdk, siblings copied from `/opt/ur24`), symlink `connect` → tree.
  - `GOOS=linux GOARCH=amd64 CGO_ENABLED=0 GOFLAGS=-mod=mod`. Mac builds have no pprof and no key persistence.
- Client: `git archive <branch>` → apply winpatch (re-add the `envpkg`/`strconvpkg` import if winpatch misses it) → `go build -modfile=<tree>.mod` in `urw/proxy/tunclient`.

**Traps hit this round**
- `pgrep -f` matches its own ssh command (start the sink with `setsid`).
- Background Bash jobs get killed under Mac memory pressure: run matrices detached with `nohup ... & disown`.
- GitHub GraphQL 502 on `gh pr create`: create through `gh api repos/.../pulls -X POST` after checking the PR list, to avoid duplicates.
- `t.Cleanup(func(){ SetWindowSizing(DefaultWindowSizing()) })` reads the global at cleanup time and leaks the rule into later tests; capture the value first (fixed in #213).
- The receiver window is clamped by the client's advertised hold. An unbudgeted rig client hides upstream's gain, so give it a budget.

---

## 3. Measurements (medians Mb/s; TUN; 30 s; synthetic origin in the provider)

### 3.1 Upstream vs beta (P38)

| Path | Flows | Beta b7 | UP (budget 384) | UPnb | MG (beta + upstream, budget) |
|---|---|---|---|---|---|
| 0.3 ms | 8 | 762 | 298 | 281 | 286 |
| 0.3 ms | 1 | 839 | 254 | 262 | 250 |
| 100 ms | 1 | 128 | 196 | 160 | 203 |
| 100 ms | 8 | 127 | 79 | 155 | 95 |

Upstream arms at 0.3 ms: host CPU 0.3–0.6 cores, i.e. starved.

### 3.2 Rollback attribution (P39, P40): same binaries, env `URNETWORK_WINDOW_SIZING=constant` on both ends

| Path | Flows | UP rule → const | MG rule → const |
|---|---|---|---|
| 0.3 ms | 8 | 261 → 466 | 270 → 656 |
| 0.3 ms | 1 | 246 → 593 | 251 → 772 |
| 100 ms | 1 | — | 198 → 126 (rule +57%) |
| 100 ms | 8 | — | 80 → 129 (rule −38%) |

**The rule causes the low-RTT collapse.** With the rule off, our fixes on top of upstream add +41% (8 flows) and +30% (1 flow).

### 3.3 Final four stacks (P44)

| Path | Flows | Beta | UP | A (#213) | B (#214) |
|---|---|---|---|---|---|
| 0.3 ms | 8 | 689 727 673 693 → **691** | 324 267 310 264 → 288 | 642 622 623 602 → 622 | 725 737 718 710 → **722** |
| 0.3 ms | 1 | 804 807 871 382 → 806 | 217 270 249 248 → 248 | 193 730 91 302 → 248 | 764 550 850 805 → **784** |
| 100 ms | 1 | 134 130 132 | 2 204 197 | 130 127 131 | 127 17 50 |
| 100 ms | 8 | 129 128 129 | 83 81 78 | 124 132 130 | 126 129 129 |

Binaries:

| Arm | Provider | Client |
|---|---|---|
| UP | `d1606c75172f` | `968a8bd307bb` |
| A | `99de16bbe8d5` | `62489e7802cd` |
| B | `0b1b99a61d86` | `c8c5ecde29c3` |
| Beta | `5479b9ef4c52` | `a7750b6a35c6` |

### 3.4 Window sweep on top of the loss fix (P31): constant window forced

| Flows | 2 MiB | 4 MiB | 8 MiB | 8 MiB, no fix |
|---|---|---|---|---|
| 8 | 639 | 613 | 499 | 383 |
| 1 | 745 | 322 | 449 | — |

- Loss (resends − duplicates) grows with window: ~180 → ~505 → ~635 /s.
- **A bigger window does not pay on the relay path.** Candidate: relay `resident.go` `processClientForward`, non-blocking 4096-message forward queue, `ForwardTimeout` 0.

### 3.5 Our fixes, standalone evidence

| Fix | Evidence | Ledger |
|---|---|---|
| Gap acks `b7ec6b80` / `1317530a` | Production A/B, 8 flows 600 → 709 (+18%, 6/6), 1 flow 707 → 812 (+15%, 4/4). Injected +0.5% loss cost 33%. Per-hole: ~18 holes/s wait one 10 ms interval. | P19–P28 |
| Standby `a94ad7d0` / `dd976e15` | Restart → first fetch 22.2 → 2.2 s (10/10). | P17–P18 |
| Group merge `3bcd5aba` | Upstream websocket messages 41–50k → 12.5–13k /s, +13% at 8 flows (4/4). ACK *dropping* (unsafe) was +12% but wedged single flows. | P32–P36 |

---

## 4. Open issue: intermittent single-flow wedge (all stacks; P42–P44)

**Symptom:** at high single-flow rates, the download stops for the rest of the run. Hosts idle.

**Captures** (`cs1-r1`, `cs2-r1` on beta b7; `pa1-r2` on A):
- Every provider `SendSequence` goroutine is parked in its normal idle select (transfer.go ~7417; ack worker ~6698).
- Client kernel socket:

| Field | Value |
|---|---|
| State | ESTAB |
| `r` (queued unread) | 3.4–3.6 MB |
| `rcv_ooopack` | ~2,800–2,900 |
| `rcv_ssthresh` | 2096 |
| `lastrcv` | ~4.5 s |

**Mechanism.** One inner segment is missing and is never retransmitted.
- The provider terminates TCP and relies on Transfer; its user TCP has SYN handling, not data retransmission (`ip.go`).
- Any inner loss after the provider emits is permanent.

**Routes**

| Route | Status |
|---|---|
| (a) Old receive path evicts already selectively acked items, then 60 s `SelectiveAckTimeout` (dev §3.11c) | Upstream committed-prefix addresses it |
| (b) Unknown | Still wedges on A. Client kernel `TcpExtTCPBacklogDrop` 4,064 cumulative, `PruneCalled` 3,624, no softnet drops; 8 healthy runs showed 0 backlog drops, so not confirmed. |

**Withdrawn hypothesis:** "contract acquisition blocks the send loop" (P42). Goroutines show no block, and halving contract rotations did not help (128 MiB 1/8 low vs ~205 MiB effective 3/8).

**Candidate directions (not started):**
- Capture `nstat` deltas on a stalled run.
- Record inner packet counts at TUN write vs kernel receive around a stall.
- Consider provider-side retransmission on duplicate ACK/SACK for the user TCP, or client-side detection of an inner hole.

---

## 5. Test status

**macOS, beta option A (`1e34c6fc` equivalent)**
- `TestSizedWindowIsComputedFromTheMeasuredRoundTrip` fails. It also fails on clean upstream `3a593216`.
- `TestSizedWindowShrinksWhenThePathShrinks` and `TestAtEquilibriumOccupancyIsHalfTheWindow` failed once under load and pass 3/3 in isolation.

**#213 agent suite:** same pre-existing failure. The contract-ahead row was made rule-explicit.

**#214 agent suite:** ok (1448 s). `TestProviderReturnsRideTheClientsLane` flaked twice in concurrent targeted runs only.

**Linux full suite NOT re-run on the final branches.** Earlier Linux runs: gap-ack branch ok apart from the pre-existing `TestWebRtcFastPathCountsReceiveQueueDrop` flake; standby branch ok.

---

## 6. Caveats agents must carry

- n = 3–4 per cell. The single-flow wedge lands in different arms run to run, so f1 medians are noisy. Quote per-run values.
- The rig client is not an sdk device. The desktop budget was emulated with a process budget only.
- Upstream (UP) was built natively on the VPS with cgo; A and B were cross-built on the Mac with cgo off. The rollback pairs (P39/P40) are same-build.
- Upload was not measured for the upstream stacks.
- The Germany host has environmental stalls (P8).

---

## 7. What to test on beta (A) and expected

| Test | Before (old beta b7) | After (A) |
|---|---|---|
| Same datacenter, 8 flows | ~690 Mb/s | ~620 |
| Same datacenter, 1 flow | ~806 Mb/s | ~750–800 when not wedged; wedge may occur |
| 100 ms, 1 and 8 flows | ~130 | ~130 |
| 100 ms, rule on via `SetWindowSizing(WindowSizingFromDelivery)` | — | single flow ~200; 8 flows ~80 (known) |
| Provider restart, first client fetch | ~2 s with the standby fix | ~2 s (unchanged) |

---

## 8. Hand-off rules

- Wait for the user and dev team views before new work.
- If asked to switch beta to the best-bandwidth build: fast-forward beta to #214's code, i.e. merge `pr/aggressive-upstream-group-merge` into beta.
- DO-NOT-RETRY additions:
  - Steady faster acks (row 17) remain dead; the gap-event wake is different.
  - Constant window above 2 MiB on the relay path (P31).
  - Contract size changes for the wedge (P42).
  - ACK dropping in the client (P33, unsafe).
- Commits: author `Ryanmello07 <67509637+Ryanmello07@users.noreply.github.com>`, trailers as in session. No IPs, hosts or IDs in committed docs.
