# Per-run traces behind the numbers in LEDGER-EXCERPT.md rounds P46 through P53

Every file is the raw capture of one run, copied from the orchestrator's run directory. Client and provider ids are
replaced by `id-N` and host addresses by `host-N`, stable across the whole directory (see `IDENTITIES.md`); the
synthetic origin `198.18.0.1` is kept. Nothing else is edited. The one-line result per run is in the matching `.out`
file; the md5 in each result line is the executed provider binary (see `../README.md` §3 for the tag -> tree mapping).

| Files | Round | Runs | What they are |
|---|---|---|---|
| `after-deg3.out`, `wd-h1-*.txt` | P49 (dev H1 decided) | `h1-R1/R8` rule on, `h1-K1/K8` constant 2 MiB; 0.3 ms path; r1, r2 | provider-side per-second `SendWindowEstimate` log (`WDIAG`: writes/s, resends/s, window, sized, reason, delivered, interval, rtt, samples, floor, ceiling, obtainable, targetBound, rttMean, rttMin) |
| `after-deg3.out` (`wg1-*` lines), `cli-ss1-wg1-*.txt`, `cli-rate-wg1-*.txt`, `pcpu-wg1-*.txt`, `rcpu-wg1-*.txt` | P50 (wedge loss site) | 12 single-flow runs, b54f9f72 provider, kernel-TUN client, 0.3 ms path | per-run client-host `nstat` before/after in the `.out` line; `cli-ss1`: `ss -tinm` of the client's sockets taken at the rate collapse (inner socket `skmem` `d` = `sk_drops`); `cli-rate`: per-second NIC rate; `pcpu`/`rcpu`: provider / client cores over the run |
| `deg2.out`, `rollbackde.out` | P46 | Germany (~101 ms) client: rule off vs on, 1 and 8 flows | result lines with the client websocket socket summary |
| `deg3.out`, `wd-dg3-*.txt` | P48 (rule's 8-flow runaway) | `dg3-R1/R8` rule on, `dg3-C41/C48` constant 4 MiB; Germany; r1, r2 | `WDIAG` per second: the windowed RTT minimum inflating with the window (R1) and the 8-flow runaway (R8 r2) |
| `sweepde.out`, `wd-sd{1,8}-w{2,3,4,6,8}-r{1,2,3}.txt` | P52 (constant-window sweep at 100 ms) | 1 and 8 flows x 2/3/4/6/8 MiB x 3 rotated runs | `WDIAG` per second; the collapsed runs (`sd8-w3-r3`, `sd8-w4-r2`, `sd8-w4-r3`, `sd8-w6-r1`) show writes ~3k/s from the first second, zero resends and the ack round trip climbing to seconds |
| `wd-gd{1,8}-{R,RL,RG,RB,K4}-r{1,2}.txt` | P53 (rule variants at 100 ms) | R shipped rule, RL lifetime-minimum RTT, RG + inflation guard 1.5, RB + BDP cap 2, K4 constant 4 MiB | `WDIAG` per second with `rttLife` and `inflation-guarded` markers where the variant applies; `gd1-K4-r2` is the single-flow collapse of P56 |

Reading a `WDIAG` line: `window` is the estimate applied to the send sequence; `reason` names the binding term
("delivery", "the target", "peer's advertised capacity", ...); `rtt` is the estimate's round trip (the windowed minimum),
`rttMin`/`rttMean` are the sequence's sampled minimum and mean over the last second; `delivered`/`interval` are the
delivery term's inputs; `targetBound` is true when the 1 Gb/s target cap was computed.
