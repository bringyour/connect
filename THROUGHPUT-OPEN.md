# Throughput program: open work items

Companion to THROUGHPUTFIX.md (design sections) and
tests/PERFVAR-MEASUREMENTS.md (campaign ledger).
Updated 2026-09-14.

## CONFIRMED (2026-09-14)

The BDP hypothesis is measured. Raising LATENCY brings the regime down to the
cell, so no instrument reaching 665 Mb/s was ever required.

| RTT | flow | fixed | adaptive | paired multiple | better |
|---|---|---|---|---|---|
| 200 ms | 16 MiB | 68.2 | 117.6 | 1.707 | 10/10 |
| 400 ms | 16 MiB | 35.0 | 60.0 | 1.716 | 10/10 |
| 200 ms | 64 MiB | 70.3 | 149.7 | 2.128 | 7/7 |

Latency-invariant to 0.5%. Rule defaults OFF.

## In flight

| # | Item | Owner | State |
|---|------|-------|-------|
| 1 | Asymptote: does the multiple keep growing with transfer size? | harness | Running. Steady-state solve implies ~2.4 |
| 2 | No-op guard at low RTT, with memory readings | harness | Queued. Decides shippability |
| 3 | Cell C: which inner window sets the 4 MiB plateau | harness | Queued. Halve, never raise |
| 4 | Full race suite on the window + reply-key commits | implementer | Pending |
| 5 | Should the rule stop climbing when delivery stops responding? | implementer/designer | Pending |
| 6 | §36: calibration, the H3 exclusion | designer | Pending |
| 7 | What the rule would compute on the REPORTER'S path | designer | Pending. The payoff question |
| 8 | Namespace cell | harness | PAUSED. Bare forwarder does 3,984-4,271 Mb/s, 6x the bar |

## Open questions with no owner yet

| # | Item | Note |
|---|------|------|
| 9 | Why 8 flows = 1.84x 1 flow, identically with and without Transfer | Unexplained. Suggests both cells share a bound |
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
- H3 stream window as the 4 MiB plateau — designer refuted its own prediction:
  3.24 MB goodput in flight cannot come from a 3.15 MB framed window, and the
  fixture's carrier is in-process (no QUIC, no carrier socket)
- Window collapse as the cliff mechanism — rung flat, duration exploded
