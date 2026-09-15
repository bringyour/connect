# rig: the measurement harness, diagnostics and run ledger behind PR #213 / #214

This directory is the sanitized, self-contained copy of what produced the numbers in
`THROUGHPUT-RIG-REVIEW.md`, `REPORT-2026-09-15-UPSTREAM-WINDOW-RULE-AND-PRS.md` and the ledger excerpt.
Nothing here is built into the package: `rig/` has no `.go` files, the diagnostic Go sources are stored
as `.go.txt`, and the scripts run over ssh from an operator machine. Reviewers asked for the raw ledger,
the harness sources and the config manifests (ledger P47); this is that.

Every host address, platform hostname, identity and credential path has been replaced by a placeholder.
`env.example` lists them; copy it to a private file outside the repository, fill it in and `source` it.
The scripts fail fast (`${VAR:?}`) when a value they need is missing.

## 1. The rig

Three physical hosts plus the shared beta relay:

| Role | Placeholder | Where | What runs there |
|---|---|---|---|
| Provider | `$PROVIDER_HOST` | datacenter A | `urprovider` as a systemd unit (`/usr/local/bin/urprovider`; alternate builds kept as `/usr/local/bin/urprovider.<tag>`), pprof on `127.0.0.1:6060`, `cpuwin.sh`, `stallwatch.sh`, `zerohttp.py` |
| Near client | `$CLIENT_HOST` | datacenter A, ~0.3 ms network RTT to the provider | the rig clients (`/tmp/urtun-<tag>`, `/tmp/ursocks-<tag>`), `cmeas.sh`, `tmeas.sh`, `umeas.sh`, `ttfb-client.sh`, `cpuwin.sh`, `stallwatch.sh`, `wssample.py` |
| Far client | `$FAR_CLIENT_HOST` | ~100 ms from the provider | the same client scripts, plus `sink.py 9420` for upload tests |
| Relay | `$RELAY_HOST` (= `connect.$PLATFORM_DOMAIN`) | shared beta platform | the H1 websocket relay every client and provider connects to. **Read-only, never restarted**; the rig only samples its CPU (`cpuwin.sh` on the `server-connect-1` container) |

Traffic path for every download run: client (TUN or socks) -> H1 websocket to the relay -> provider ->
origin. Both rig clients run relay-only (`URNETWORK_NO_P2P=1`, see `diag/winpatch.py`), so no run depends
on whether direct P2P happened to come up. Every client run pins `PROVIDER_ID` so the platform cannot
hand the flow to another provider.

Origins:

- **Synthetic (`synth` target).** `http://198.18.0.1/download/<bytes>`. Any TCP flow whose destination
  falls in the RFC 2544 benchmarking range `198.18.0.0/15` is terminated inside the provider's local NAT
  by the package's built-in in-memory server (`ip_synthetic_speed.go`: a `net.Conn` that streams
  `<bytes>` from memory, no kernel socket, no upstream dial); the rig uses `198.18.0.1`. The download
  side of the rig therefore exercises the whole tunnel and nothing beyond it.
- **Kernel-socket origin.** `zerohttp.py PORT` is the same `GET /download/<bytes>` contract served by a
  Python HTTP server bound on a real socket, for the runs that needed the provider's upstream TCP socket
  in the path (ledger T15, P1).
- **CDN (`cdn` target).** A public CDN test file, resolved once into `$CDN_IP`; used only as a
  real-Internet control.
- **Upload sink.** `sink.py 9420` on the far host prints one line per second
  (`total=<bytes> rate_mbps=<n>`); `umeas.sh` PUTs a 1.5 GB file through the tunnel to it. Start it with
  `setsid` (ledger trap: `pgrep -f` otherwise matches its own ssh command).

Client modes:

- `CLIENT_MODE=tun` (the product-like path): the scratch TUN client (`urw/proxy/tunclient`, not the sdk)
  creates `ur0`, `tmeas.sh` gives it `10.66.0.2/24`, MTU 1100, and routes `198.18.0.0/15` (and the CDN
  address) into it, so `curl` uses the host kernel TCP stack.
- `CLIENT_MODE=socks` (default of `ceil2.sh`): the socks client with its userspace netstack listens on
  `127.0.0.1:9999`; `curl --socks5` through it. Its inner receive window is ~2 MiB, which caps it at long
  RTT (ledger T16); a socks-only effect is treated as a harness effect (TASKLIST global rules).

## 2. Scripts

All orchestrator scripts are run from this directory on the operator machine; `S` in each script is the
script's own directory and is where per-run outputs land. Remote scripts are installed as `/root/<name>`
on the host named in the table above.

### The run primitive (orchestrator)

`ceil2.sh LABEL PROVIDER_TAG FLOWS DUR TARGET [CLIENT_ENV] [PROVIDER_ENV_LINE]`

1. `cleanleaks.sh`: removes leaked socks5 client registrations through the platform API (a killed socks
   run leaks ~2.7 registrations; >100 top-level clients silently disables peer discovery for the network).
2. Installs `/usr/local/bin/urprovider.<PROVIDER_TAG>` as the provider, writes `PROVIDER_ENV_LINE` into a
   systemd drop-in (`exp.conf`) or removes it, restarts the unit, settles 25 s.
3. Records the provider md5 and `z0`, the number of goroutines parked in `acquirePackAdmission` (the
   zombie-flow counter; must be 0 before and after).
4. Samples provider and relay CPU over the measurement window (`cpuwin.sh`, offset to skip warm-up).
5. Runs the client via `tmeas.sh` (TUN) or `cmeas.sh` (socks) on `$CLIENT` (default `$CLIENT_HOST`;
   `CLIENT=root@$FAR_CLIENT_HOST` for the long path), then records `z1`.

Env: `CLIENT`, `CLIENT_MODE`, `SOCKS_BIN`, `TUN_BIN`. One output line:
`LABEL TAG f=FLOWS TARGET client=host/mode | bin=<client md5> goodput=<Mb/s> wire=<NIC Mb/s> el=<s> client[cores=.. hot=..% threads>50%=..] provider[...] relay[...] | md5=<provider md5> z0=.. z1=..`

`tdone.sh LABEL FLOWS TARGET "PROVIDER_ENV_EXTRA"` runs `ceil2.sh` with `URNETWORK_TDIAG=1` on the
provider (env `PB`, `DUR`, `CE` for tag, duration, client env), pulls the provider's `TDIAG` lines into
`td-LABEL.txt` and appends the parsed gate line from `tdparse.py`.

### Client-host scripts (installed on both client hosts)

| Script | Runs | Reports |
|---|---|---|
| `cmeas.sh LABEL FLOWS DUR synth\|cdn [ENV]` | socks client, `FLOWS` parallel curl loops for `DUR` s after a 6 s warm-up | goodput (sum of curl bytes / actual elapsed), wire rate from the NIC counters, process CPU (`cpuwin.sh`) |
| `tmeas.sh LABEL FLOWS DUR synth\|cdn [ENV]` | the same with the kernel-TUN client (pprof on `127.0.0.1:6061`) | the same plus `tun write errors` |
| `umeas.sh LABEL DUR SINK_HOST` | socks client, one curl upload of `/tmp/up.bin` to `SINK_HOST:9420` | `curl_speed_upload`, START/END timestamps to match against the sink's per-second lines |
| `ttfb-client.sh LABEL` | starts a socks client and retries a 1 MiB fetch | seconds until the first successful fetch, `STALL60` if none within 60 s |
| `cpuwin.sh PID DELAY WINDOW` | reads `/proc/PID/task/*/stat` twice | `cores=` (process), `hot=` (hottest thread %), `threads>50%=` |
| `stallwatch.sh LABEL IFACE tx\|rx PPROF_PORT SECONDS` | watches the NIC rate once per second | after the rate exceeded 300 Mb/s and then stays under 50 Mb/s for 4 s: one goroutine dump, `ss -tinm` twice, `nstat` TCP counters (`/tmp/sw-LABEL.*`) |
| `wssample.py LABEL SECONDS` | once per second, `TCP_INFO` of the busiest established `:443` connection to `$RELAY_HOST` | `/tmp/ws-LABEL.jsonl` (cwnd, rtt, retrans, rcv_space, skmem, delivery rate) |

Every client run leaves its log at `/tmp/tm-LABEL.log` (TUN) or `/tmp/cm-LABEL.log` (socks) on the
client host; the matrices count `evaluation ping timeout` lines in it as `stall=`.

### Matrices and A/B drivers (orchestrator)

Each driver rotates arm order per repetition, restarts the provider per run, skips labels already
present in its `.out` file (resumable), and prints one line per run with the md5 of both executed
binaries. Run them detached (`nohup ... & disown`): background jobs died under memory pressure on the
operator machine.

| Script | Arms | Question (ledger) |
|---|---|---|
| `upmatrix.sh PFX CLIENT FLOWS REPS` | BETA / UP / UPnb / MG | upstream window rule vs beta, with and without a process budget (P38) |
| `prmatrix.sh PFX CLIENT FLOWS REPS` | BETA / UP / PA / PB | the final four stacks: beta, upstream, PR #213, PR #214 (P44) |
| `rollback.sh` | UPr / UPc / MGr / MGc | same binaries, rule on vs `URNETWORK_WINDOW_SIZING=constant` on both ends, same datacenter (P39) |
| `rollbackde.sh` | MGr / MGc | the same from the far client (P40) |
| `gpfinal.sh` | base / fix | production A/B of the gap-ack fix, f8 n=6 and f1 n=4 (P28) |
| `sackab.sh` | ctl / S / R / SR / SR2 | sorted selective acks, sacked-byte release, larger receive queue (P22-P24) |
| `gapab.sh` | ctl / G / SG / SR2G | gap-triggered ack wake alone and combined (P25) |
| `sgconf.sh` | ctl / SG | confirmation of the SG combination, f8 n=6, f1 n=4 (P26) |
| `dropdose.sh [N]` | 0 / 0.5% / 2% injected loss | dose-response of relay-like message loss (P19) |
| `dropmech.sh` | the same doses with TDIAG/RDIAG/XDIAG | where the loss cost is paid (P20-P21) |
| `winsg.sh` | w2sg / w4sg / w8sg / w8off | constant-window sweep on top of the loss fix (P31) |
| `mergeab.sh` | M0 / M1 | upstream group merge (`URNETWORK_DIAG_GROUP_MERGE=1`), websocket messages per second (P32-P36) |
| `acoab.sh` | D / K / C | TUN read loop: direct read, read channel, channel + ACK coalescing (P37) |
| `bldrop.sh` | PA, f1, n=8 | client kernel `TcpExtTCPBacklogDrop` / `PruneCalled` / OFO deltas around single-flow stalls (P43) |
| `catch.sh`, `catch2.sh` | f1 loops, max 14 | stall catcher: `stallwatch.sh` on both ends, stops at the first run with a dump and goodput < 600 (P42-P43); `catch2.sh` also fetches `ss`/`nstat` |
| `startab.sh [N]`, `startfix.sh [N]` | A / B provider builds | provider restart then immediate client: time to first fetch, standby dial and verification-exit counts (P17-P18) |
| `keyrace.sh`, `keyfix.sh` | client starts 0 s vs 60 s after restart | the provide-key race and its persistence prototype (P14-P15) |

### Parsers (orchestrator; read the `.out` files and the `td-`/`cl-` capture files)

| Script | Input | Output |
|---|---|---|
| `tdparse.py td-LABEL.txt` | provider `TDIAG` lines | per run: loops/s, time-weighted `noCap` / `otherNil` / `starve` / `work` %, resend-queue average and max, resends/s |
| `upstat.py FILE PREFIX` | a matrix `.out` | per flows and arm: the goodput list, median, stall total |
| `skstat.py` | `sackab.out` + captures | goodput, gate share, resends, client blocked %, spurious resends per arm, paired % vs ctl |
| `wsstat.py` | `winsg.out` + captures | the same for the window sweep, plus in-flight bytes |
| `lossloc3.py PROVALL CLI PROV` | `XDIAG` on both ends | large websocket writes (provider) vs reads (client) over the common steady seconds: the loss localization (P16) |
| `stages.py PROV CLI` | `TDIAG` + `XDIAG` + `RDIAG` | frames at each client stage (websocket read -> unwrap -> pack -> sequence) and the gap between stages |
| `holes.py CLI` | `LDIAG` hole histograms | holes/s by duration class and the blocked share of wall time |

## 3. Run naming

`<prefix><flows>-<arm>-r<rep>`; the prefix identifies the driver, the arm the binaries and env, `r` the
repetition (arm order rotates with it). Examples from the ledger: `up8-UP-r2`, `pr1-PA-r3`,
`rb8-MGc-r1`, `rd1-MGr-r2`, `gp8-fix-r4`, `sk-SR2-r3`, `ga-SG-r1`, `sg1-ctl-r2`, `ws8-w4sg-r2`,
`dd-5000-r1`, `dm-20000-r2`, `mg8-M1-r3`, `ac1-C-r2`, `bd1-r4`, `cs2-r7`, `ab-A-r1`, `fx-B-r2`,
`kr-d60-r3`, `kf-d0-r8`.

Per label the rig keeps: the client log on the client host (`/tmp/tm-LABEL.log` or `/tmp/cm-LABEL.log`),
provider and relay CPU (`pcpu-LABEL.txt`, `rcpu-LABEL.txt`), the provider's diag lines (`td-LABEL.txt`),
the client's diag lines (`cl-LABEL.txt`), stall captures (`prov-gr-`, `cli-gr-`, `cli-ss1-`, `cli-ss2-`,
`cli-nstat-LABEL.txt`), and the one-line result in the driver's `.out`.

Provider build tags are the suffix of `/usr/local/bin/urprovider.<tag>`: `b7` (beta at `b7ec6b80`),
`up`/`up2` (upstream main), `mg`/`mg2` (beta + upstream merge), `pa` (PR #213), `pb` (PR #214),
`b-ctl`, `b-keys`, `b-keys-sb`, `b-keys-fix` (startup work), and the diag builds `tdiag`, `sackdiag`,
`ldiag`, `ldiag2`, `dropdiag`, `drop`. Client builds are `/tmp/urtun-<tag>` and `/tmp/ursocks-<tag>`
(`gp-base`/`gp-fix`, `up`, `mg`, `pa`, `pb`, `x3`..`x14` for the diag series). The ledger entry for each
run states which commit and which of the `diag/` patches a tag was built from; the md5 in every result
line is what ties a number to a binary.

## 4. `diag/`: the diagnostic patches and env-gated Go files

None of this is product code and none of it was committed to the package. The `.py` scripts patch a
scratch copy of the tree before a build (each asserts its anchors and refuses to apply twice); the
`zz_*.go.txt` files are dropped into the package as `zz_*.go` in that scratch copy. They are stored as
`.go.txt` here so they cannot compile into `connect`.

| File | What it adds | Env gate |
|---|---|---|
| `winpatch.py TREE` | relay-only switch; constant sender/receiver window overrides; logical lane count | `URNETWORK_NO_P2P=1`, `URNETWORK_WIN_MIB`, `URNETWORK_RWIN_MIB`, `URNETWORK_LANES` |
| `tdiag.py TREE` + `zz_tdiag_env.go` | time-weighted `SendSequence` gate accounting per second (`TDIAG` lines); ack-compression overrides | `URNETWORK_TDIAG=1`, `URNETWORK_ACK_COMPRESS_US`, `URNETWORK_ACK_COMPRESS_BYTES` |
| `wcount.py TREE` | on top of tdiag: writes and write errors handed to the route writer | `URNETWORK_TDIAG=1` |
| `rdiag2.py TREE` | receiver side: head / past / future arrivals by distance, duplicates, head-blocked wall time (`RDIAG`) | `URNETWORK_TDIAG=1` |
| `cdiag.py TREE` | provider congestion-drop snapshot every 2 s when it changes (`CDIAG`) | `URNETWORK_TDIAG=1` |
| `xdiag.py TREE` + `zz_xdiag.go` | H1 websocket messages written/read per second and, in the later client build, every pre-sequence drop site (`XDIAG`) | `URNETWORK_TDIAG=1` |
| `zz_ldiag.go` | latency decomposition histograms: hold, compress, ack, hole episodes (`LDIAG`) | `URNETWORK_TDIAG=1` |
| `zz_sacksort.go` | selective acks in sequence order; end the compression wait when a hole is proven or fills (the prototype of `1317530a`) | `URNETWORK_DIAG_SACK_SORT=1`, `URNETWORK_DIAG_GAP_ACK_NOW=1` |
| `zz_sackdiag.go` | selectively acked bytes stop counting against resend-queue admission | `URNETWORK_DIAG_SACK_RELEASE_KIB` |
| `zz_dropdiag.go` | discard a share of large H1 messages after route acceptance (relay-drop emulation) | `URNETWORK_DIAG_DROP_PPM` |
| `zz_groupmerge.go`, `zz_groupmerge_test.go` | merge queued single-frame logical groups into one H1 Pack (the prototype of PR #214) | `URNETWORK_DIAG_GROUP_MERGE=1` |
| `zz_wdiag.go` | per-destination send-window estimate, writes and resends once per second (`WDIAG`) | `URNETWORK_WDIAG=1` |
| `zz_windowsizing_env.go` | the documented one-call rollback `SetWindowSizing(WindowSizingConstant)` at init | `URNETWORK_WINDOW_SIZING=constant` |
| `zz_contractsize.go` | override the standard contract size | `URNETWORK_DIAG_CONTRACT_MIB` |
| `keypersist.py TREE` | prototype for the provider binary (`sn/cli/miner/run.go`, not this package): persist provide secret keys across restarts | none |

Order when several apply to one tree: `winpatch.py`, then `tdiag.py`, then any of `wcount.py`,
`rdiag2.py`, `cdiag.py`, `xdiag.py`. The provider builds carried the earlier, smaller `zz_xdiag.go` and
`zz_ldiag.go` (five H1 counters; no hole histogram); the versions here are the later client-side ones,
which are supersets.

Provider builds on the VPS: `/opt/urNN` is a copy of a sibling tree with `connect` replaced, then
`cd sn/cli/miner && GOFLAGS=-mod=mod go build`. Provider builds cross-compiled on the operator Mac use
`GOOS=linux GOARCH=amd64 CGO_ENABLED=0 GOFLAGS=-mod=mod`. Clients: `git archive <branch>`, apply
`winpatch.py`, `go build` in `urw/proxy/tunclient` with the tree's modfile.

## 5. Caveats

- **The rig client is not the sdk.** It is a scratch program (`urw/proxy/tunclient`) that writes packets
  one by one to `/dev/net/tun` (no GRO, so the kernel ACKs every ~2 segments) or runs a userspace
  netstack behind a socks listener. Attached pools and device settings differ from an sdk device.
- **Process budget emulation.** `HARNESS_MEMORY_BUDGET_MIB=<n>` makes the client call
  `connect.SetMemoryBudget` before any settings are built, the way the sdk hosts do. It emulates a
  budgeted desktop process only; it is not an sdk device. An unbudgeted rig client advertises a hold
  that hides upstream's receiver-window gain, so the upstream arms were run with a budget.
- **The relay is shared and read-only.** It is the beta platform's relay; other users are on it, it was
  never restarted or reconfigured, and only its CPU was sampled. Relay-side counters (forward-queue
  drops) could not be read; the loss localization (P16) attributes to the relay by exclusion.
- **Mac cross-builds have no pprof** (and no key persistence). For those provider builds `z0`/`z1` and
  the stall goroutine dumps come back empty; the same-build rollback pairs (P39/P40) are unaffected,
  the cross-stack comparison (P44) mixes a cgo VPS build (UP) with cgo-off Mac builds (A, B).
- **Noise.** n = 3-4 per cell; 8-flow A/A range 14%; the single-flow wedge (report section 4) lands in
  different arms run to run, so f1 medians are noisy and per-run values are quoted. The far host has
  environmental stalls (P8).
- **Every run restarts the provider and pins one provider.** Numbers are for one client on a fresh
  provider, not for a loaded provider; `PROVIDER_ID` must never be left unpinned or the platform may
  route the run elsewhere.
- **Leaked registrations.** Killing a socks client mid-run leaks its registration; `cleanleaks.sh` runs
  before every provider swap. The leak is a correctness bug (ledger D), not a throughput effect (ledger B9).
- **The CDN target** is a real-Internet control and shares the path with whatever the Internet does that
  minute; it was not used for any claim in the PRs.

## 6. Documents

- `LEDGER-EXCERPT.md`: the DO-NOT-RETRY table and rounds P16 through P56 of the live ledger, verbatim; `traces/`: the per-run captures behind rounds P46-P53 (see `traces/README.md`)
  except for sanitization. Run windows, binaries and per-run values for every number in the report.
- `REPORT-2026-09-15-UPSTREAM-WINDOW-RULE-AND-PRS.md`: the agent report that accompanies PR #213/#214.
- `TASKLIST.md`: the investigation spec (hypothesis, instrument, decision rule fixed before measuring,
  status) that the ledger entries answer.

## 7. Sanitization rule

`CODESTYLE.md` forbids production identities, hostnames, addresses and secrets in committed artifacts.
Everything here was copied verbatim and then rewritten so that: rig host addresses are `$PROVIDER_HOST`,
`$CLIENT_HOST`, `$FAR_CLIENT_HOST`, `$RELAY_HOST`; platform hostnames are derived from
`$PLATFORM_DOMAIN` (and, in two DNS notes of the ledger, `$PRODUCTION_DOMAIN`); the provider identity is
`$PROVIDER_ID`; credential files are `$JWT_FILE` (or `$JWT`); session-local paths are `$RIG_TMP`. The
only literal addresses left are `127.0.0.1`, `0.0.0.0`, the TUN-local `10.66.0.2/24` and the benchmarking
range `198.18.0.0/15` with the fixed test origin `198.18.0.1`, none of which identifies a live system.
