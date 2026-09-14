# Throughput program: open work items

Companion to THROUGHPUTFIX.md (design sections) and
tests/PERFVAR-MEASUREMENTS.md (campaign ledger).
Updated 2026-09-14.

## Blocked on one instrument

Three questions need a cell that reaches the reporter's regime ON A DOWNLOAD.
No cell we have does; every one tops out 3-4x below their single-flow figure.

| # | Item | Owner | State |
|---|------|-------|-------|
| 1 | Namespace cell: kernel-stack client, real tun | harness | BUILDING. Real TUN works in container; named netns refused, testing `unshare --net` vs `--privileged` |
| 2 | Acceptance test for it: 1 flow, download, no provider, near 665 Mb/s | harness | Defined, not met |
| 3 | BDP hypothesis (user's): queue / effective ack RTT | — | UNTESTED, not refuted. Cell tops out ~190 Mb/s vs a predicted 671 ceiling |
| 4 | Writer-occupancy test of the zombie/BDP unification | harness | Queued behind #1. Readers exist |

## Open questions with no owner yet

| # | Item | Note |
|---|------|------|
| 5 | Why 8 flows = 1.84x 1 flow, identically with and without Transfer | Unexplained. Suggests both cells share a bound |
| 6 | Structural decomposition: where the 113x kernel-vs-our-path goes | Designer; partly superseded by the BDP work |
| 7 | Lane rollout decision set | Needs reply-key change + floor + lock fix together. Provider-side only |

## Ready, awaiting a decision

| # | Item | State |
|---|------|-------|
| 8 | Merge the branch as correctness work | 74 commits, suite green, cross-compiles pass. Repairs a regression now on main |
| 9 | Questions to send the reporter | Drafted: WireGuard at 1 flow; their rmem/wmem integers; whether their client runs our tun |
| 10 | THROUGHPUT-REPORT.md | Drafted ~1090 lines in scratchpad, not yet written to the repo |

## Deferred, with reasons

| # | Item | Why |
|---|------|-----|
| 11 | Receive-direction matrix (3 arms x 4 budgets) | Restates two budgets already measured; ~45 min |
| 12 | Bistable window incidence, 40 reps | Prediction recorded; not a multiple |
| 13 | Claim-2 loss instrument | Needs seeded inner-path loss; adjacent finding |
| 14 | Lane arm re-run | Inert until the reply-key change lands |
| 15 | Abandon false-positive rate, 20 reps | Arms need synthesising from a recent base |
| 16 | 24 MiB memory ceiling | Deferred by the user in the prior program |

## Killed by measurement (do not revisit without new evidence)

- 4.5x from latency we add — was the harness's own serial delay element
- Pool pinning explains the threshold — failed its own falsification
- Per-destination reliable bound as the cell's figure — cell has no Transfer layer
- Window cap model — 64x range moved rates 3x, non-monotonically
- Client receive window as the download limiter — flat across 32x
- Retransmission as the cliff mechanism — zero spurious recovery
- Window collapse as the cliff mechanism — rung flat, duration exploded
