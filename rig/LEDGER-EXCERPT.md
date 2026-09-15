# LEDGER excerpt: DO-NOT-RETRY table and rounds P16 through P47

Verbatim from the live investigation ledger (`LEDGER.md`, 2026-09-13 .. 2026-09-15), sanitized: host addresses, platform
hostnames, identities and local paths are replaced by the placeholders listed in `env.example`. Run-level detail for the
numbers quoted in the report and in PR #213/#214 lives here.

## DO NOT RETRY — closed hypotheses, with the reason each died
Authoritative list as of 2026-09-13. Chronological detail for each entry is further down this file.
RULE (learned the hard way, cost one public retraction): before proposing anything that resembles a line
below, read the WHY column. "It was killed" is not the useful part; the mechanism that killed it is.

### A. The governing fact that kills a whole class at once
A single flow is **serialized, not cycle-starved**. One flow pins ~1.05 cores while ~2 cores sit idle at
8 flows. Profile at full rate: syscalls 45%, Go scheduler findRunnable 22%, real data work (checksum,
memmove, alloc) under 4% combined, AES ~1%, futex 74.7% of syscall time.
=> **Any proposal whose mechanism is "use less CPU / fewer allocations / fewer syscalls" cannot raise
single-flow throughput.** The cycles it frees are already idle. Do not re-derive this.

### B. Tested and refuted
| # | hypothesis | why it died |
|---|---|---|
| 1 | Enlarge the wire Pack beyond one MTU | The mechanism ALREADY EXISTS for H1: sendPackH1GroupMaxFrames=16 and sendPackH1EstablishedMaxMessageByteCount=3*DefaultMtu (transfer.go:1026-1033), granted when policy.h1Only (transfer.go:2228-2234). The proposal misread the code. |
| 2 | Send whole chunks on readPackets instead of per packet | Removes ZERO syscalls; ceiling ~0.08% of a core; and as specified it introduces an aliasing bug. |
| 3 | Raise the SOCKS relay buffer / hoist deadlines | That is the TEST HARNESS, not the product path. Arithmetic was inverted; ~3% of a core; the deadline half is a correctness regression. |
| 4 | Batch the per-frame route scan | Removes zero wakeups; ceiling 0.05-0.2% of a core. |
| 5 | MTU tuning | Measured sweep: 1100=224 Mb/s (default, optimal), 1150=210 (-6%), 1200/1250/1300=197 (-12%), 1420 = tunnel passes NO traffic at all. There is no upside above 1100 and it breaks above 1300. |
| 6 | Force a different transport for speed | H1 is IDENTICAL to Auto (219/210 vs 210/210 at 1 flow). H3/QUIC carries nothing on beta because the beta relay has no QUIC listener — unmeasurable there, NOT broken in product. |
| 7 | Logical lanes / stripe one flow across lanes | Refuted in interleaved A/B with per-run cleanup: baseline median 1342 vs lanes 1091 Mb/s. Structurally cannot help ONE flow anyway — lanes shard by 5-tuple. |
| 8 | Batch receive delivery (WriteBatch instead of per-packet Tun.write) | Null at 1 flow (210/201/201 vs 219/201/201). At 8 flows one of three runs COLLAPSED to 156 Mb/s with 2339 ms latency. Refuted on its central prediction. |
| 9 | Leaked client registrations cost throughput | Refuted by an earlier deliberate dose-response (+400 leaks): throughput does NOT track leak count. The leak is a REAL bug (see CLIENT-REGISTRATION-LEAK.md) but it is a correctness bug, not a throughput bug. I re-proposed this once against my own killed table and had to retract it publicly. |
| 10 | ~~p2p lane active vs disabled slows healthy throughput~~ **VOID 2026-09-13, see section I — the valve was tripped so p2p never activated** | Null: 655 vs 635 Mb/s median over 6 interleaved runs. NOTE the distinction that matters — forced p2p DESTABILISES (it is the collapse mechanism, now fixed) but it does not SLOW a healthy path. |
| 11 | Client binary version (early build vs current) | Null: 599 vs 615 Mb/s interleaved under identical conditions. |
| 12 | A bigger constant send window | CONDITIONAL, and that is why a constant is wrong. 16 MiB gave +46% when the path had headroom; in a SATURATED period the same window lost throughput and doubled latency (rep1 51->57->45 Mb/s, latency 1151->2633 ms). A fixed constant cannot be right for both regimes. Any window work must be BDP-sized with a congestion response, and 16 MiB is not shippable to phones (mobile floor is 256 KiB = ~12 Mb/s cap). |
| 13 | Frames silently dropped before the sequence | DEAD by direct measurement: unwrapErr=0 badProto=0 pathMismatch=0 streamDrop=0 readErr=0 on BOTH ends for a whole collapse run. |
| 14 | ack-credit deadlock (acks lost or mis-credited) | Refuted: all ledger counters 0; the frames were not arriving at all. |
| 15 | Provider pool pollution | Killed: the pool requires reliability.connected=true and resets within 60 s. |
| 16 | Raise the send window to break the post-fix ~650 Mb/s ceiling | 2026-09-13 (M3): 8 MiB (receiver matched) gave 612/548/609/660 with in-flight 4x and resends 5x = saturated regime. Raising ONLY the sender window wedges the flow (M4). |
| 17 | Faster Transfer ACKs (AckCompressTimeout cut, byte-OR-time ack) as a product throughput fix (steady rate; NOT the gap-event wake + ordered selective acks, which shipped as b7ec6b80, P28) | 2026-09-13 (M10-N7, N11): real +16-20% on the socks harness, but kernel-TUN (product-like) +5% median inside noise, byte clock raises TUN resends ~50%, and long-RTT stall rate 9/17 vs 7/27. Parked on branch exp/ack-byte-clock. |
| 18 | Enable the H1 ACK priority lane for non-mobile clients | 2026-09-13 (N11): TUN 583 -> 602 median (+3%, 3/4), inside noise. |
| 19 | "The client process is the per-client limiter" | 2026-09-13 (M8): true only for the socks harness under an 8 MiB window; the TUN client is data-starved (transport reader 66-73% blocked in netFD.Read). |

### C. Retracted claims of my own
- "192 leaked registrations cost ~10x throughput" — WITHDRAWN. Refuted by my own dose-response data, which
  I had already recorded as REFUTED before re-proposing it.
- "H3 has a production reachability bug" — CORRECTED. A real quic-go probe with Google/Cloudflare positive
  controls shows production connect.$PRODUCTION_DOMAIN completes a QUIC handshake in 66 ms with NO ALPN, which
  is exactly what the product sends. My first hand-made QUIC Initial probe was worthless — Google and
  Cloudflare ignored it too. The beta relay simply has no QUIC listener.
- Two harness errors that inflated results before 2026-09-11: dividing bytes by the NOMINAL window instead
  of actual elapsed (inflated urnetwork up to 45%, barely touched WireGuard), and a latency probe that
  downloaded the same 200 MB file and stole a flow's worth of bandwidth.

### D. Still open, deliberately not closed
- Client registration leak: ~2.7 registrations per killed socks run; >100 top-level clients in 14 days
  silently disables peer discovery network-wide (LimitTopLevelClientIdsPerNetwork=100,
  network_client_model.go:47 -> NetworkPeersEnabled, peer_model.go:342-376). Real bug, needs a design call
  on where to persist by_client_jwt. NOT a throughput bug.
- Three verified micro-wins, each individually too marginal to justify shipping alone:
  parseIpPathWithPayloadBorrowed (ip.go:7362, 46.0ns/2 allocs -> 13.9ns/0 allocs); shuffled() allocating a
  fresh []Route per received frame (transfer_route_manager.go:1736-1752); relayBufferSize 2048->8192
  (proxy/relay.go:58, harness only).
- WriteBatch cross-flow ordering: tun.go:1027-1033 gives no ordering guarantee across flows, and the mobile
  SDK uses this path in production (sdk/device_local.go:4224). One collapse in three runs; n=3 cannot
  separate it from rig noise. Worth a correctness look independent of speed.



### P. 2026-09-14 autonomous round, entries P16 through P47 (P1..P15 omitted: they are the earlier upload/startup work)
  P16. T2 LOSS LOCALIZATION — PROVIDER CONSERVES, CLIENT CONSERVES, LOSS IS ON THE RELAY (decision rule met).
    Setup: kernel-TUN client, synth 8 flows, provider b-diag4 03c0e9b7d92d (beta + tdiag/wcount/xdiag), clients
    urtun-x3 3634a5e3b8d6 / urtun-x4 8ee152b45f99 / urtun-x5 d9b9106628e7 (each adds counters, no behaviour change).
    Runs (UTC, 2026-09-14): ll4 ~10:3x; ll5-r1 10:43:52-10:44:40; ll5-r2 10:45:26-10:46:13; ll6-r1 10:51:11-10:51:28;
    ll6-r2 10:52:15-10:53:02. (Provider glog prints BST = UTC+1; provider NTP unsynchronised, ~3 s ahead of the Mac.)
    TRAPS FOUND (do not repeat):
      (a) websocket message counts are not a frame count: client large ws reads exceeded provider large ws writes by up to
          +26k in one run and fell short in another. Unusable for conservation.
      (b) whole-run totals across two hosts carry boundary error of up to ~1 s of traffic (ticker phase + the final
          partial interval + in-flight at kill, 1-30k items). Totals "loss" of 0.1-0.6% is inside that error.
      (c) the earlier "client arrivals 155/s below provider writes" came from unaligned per-second medians: same flaw.
    ROBUST ESTIMATOR (alignment-free): a first send that never reaches ReceiveSequence.receive() must be resent and its
      resend fills a hole; a spurious resend arrives as "past" (or dupQueued). True loss = resends - past - dupQueued.
        ll5-r1 writes 1,180,602 resends 8,064 (selective 7,779) past 1,186 dupQ 7  -> 6,871 lost (146/s, 0.59%)
        ll5-r2 writes 1,346,568 resends 7,434 (selective 7,057) past 1,556 dupQ 4  -> 5,874 lost (128/s, 0.44%)
        ll6-r1 writes   547,225 resends 2,477 (selective 2,477) past   358 dupQ 0  -> 2,119 lost (118/s, 0.39%)
        ll6-r2 writes 1,305,700 resends 7,683 (selective 6,960) past 1,625 dupQ 0  -> 6,058 lost (132/s, 0.47%)
    PROVIDER in == out: SendSequence route writes 0 errors in every run (a route write timeout returns
      errTransferRouteWriteTimeout via writeMultiRouteWithCarrier, so writeErrs=0 means every write entered an H1 send
      channel); the H1 writer consumes the channel with no drop path short of connection close; batch flushErrors 0;
      no [ts] write errors in the provider log.
    CLIENT in == out, every pre-receive drop site counted, all 0 in all runs: H1 reliable offerReceive (blocks, never
      drops); run-loop ingress vs pack decode (runInLarge -> packReach no deficit); Client.run pack handoff drops 0;
      unwrap errors 0; ReceiveBuffer nil sequence 0; ReceiveSequence contract-missing 0; receive queue full (canQueue) 0.
      notReceived 26-34/s = the duplicates (matches "past").
    LOSS SHAPE: steady every second (100-440 selective resends/s, anti-correlated with that second's writes), no bursts,
      no transport reconnects -> per-message discard in a bounded queue, not a connection drop.
    => ATTRIBUTED TO THE RELAY (read-only, not instrumentable by us). Leading candidate from a code read of the local server
       repo (0f5e7f5; the deployed beta build may differ): resident.go processClientForward — `forward.send` (4096 msgs)
       enqueue is non-blocking in production (ForwardTimeout: 0, resident.go:474) and drops on full with
       forwardDroppedCounter (urnetwork_connect_forward_dropped_messages) + a V(1) log. 4096 msgs is ~80 ms at 600 Mb/s,
       and the 2 MiB Transfer window can exceed the downstream exchange hop's drain for that long.
       Other per-message relay drops with no counter: ForwardSequence route-write timeout (connect transfer.go, V(2) log).
    CHECK FOR THE DEV TEAM (they can read Grafana): forward_dropped_messages rate for the relay during the windows above
      should be ~120-150/s. If it's 0, the loss is at a counter-less site (ForwardSequence write timeout, exchange close).
    RECONCILES WITH P5 (T15 "relay/transports dropped ~0"): P5 counted end-to-end UDP datagram loss. UDP returns ride the
      acknowledged Transfer sequence (Ack=true on H1), so relay drops are recovered by resends and never show as datagram
      loss. P5's "~0" means "no unrecovered loss", not "no relay drops"; corrected here and in TASKLIST T15.
    COST OF THE LOSS: ~0.5% of first sends -> ~130 selective recoveries/s, each head-blocking the sequence; RDIAG blocked
      time ~6% (P-earlier). A lossless relay hop (blocking forward with bounded wait, or byte-sized queues matched to the
      Transfer window) would remove it; that is server code, not ours to change on the shared relay.

  P17. SECOND STARTUP RACE LEAD — EVERY PROVIDER START ON CURRENT CODE IS OFFLINE ~15 s (family-pinned transports, IPV6 A4).
    Found in the kf-d0-r8 stall (P15): the new provider process logged, from its first second, only
    "lookup connect-v4/-v6.$PLATFORM_DOMAIN on <public resolver>:53: no such host" (also api-v4, alt-v4, extender) until
    11:20:18 "[t]standby transport dials: no pinned transport connected for 15s", then the first successful platform dial.
    Code (connect transport_family.go, sdk network_space.go familyServiceUrl, sdk 2a22fe2 2026-09-11): a provider builds
    pinned v4/v6 transports whenever the platform url has a label to suffix; the family-agnostic standby starts DISABLED and
    dials only after StandbyDelay = 15 s with neither pinned transport connected. IPV6.md §5 lists DNS records as a
    "prerequisite for enabling pinned transports", but nothing in code gates on them.
    DNS today: connect-v4.$PLATFORM_DOMAIN NXDOMAIN; connect.$PLATFORM_DOMAIN A $RELAY_HOST; relay TLS has no cert for
    connect-v4 (alert 80). PRODUCTION: connect-v4/connect-v6.$PRODUCTION_DOMAIN NOERROR with no A/AAAA (api-v4 has an A) — the
    same 15 s would apply to a provider built from current upstream in production.
    Provider log census (425 process starts since 09-12): bimodal — 231 connected in < 2 s with 0 DNS errors, 193 exactly
    at 15.0 s. Fast/slow alternated with the interleaved A/B binaries (old merged-abandon = fast, beta 7aa3a20b builds =
    slow) from 09-13 21:49, and every start since 09-14 10:03 is slow.
    CONFOUND CHECK ON OUR OWN RESULTS: P12/P13 key-race arms ran on slow-start builds; the key race stands on its
    signature (verification exits 10/10 at D=0, and 0/10 with persisted keys on the same slow-start build), but the
    harness 25 s settle and cmeas warm-up (6 s + 2 s) overlap the 15 s offline window -> partial goodput after restarts
    (keyfix d=0 runs 392-672 vs 685-944 at 60 s; startrace 297-651).
    RESULTS.
    startrace.sh (provider b-keys 8845e7081eb2, socks client ursocks-m 94485740eaed, cmeas 1 flow 20 s, client ~1 s after
      restart, n=20, 10:57-11:14 UTC): verification exits 0/20; goodput 323 401 651 297 527 467 652 558 664 678 440 36* 672 566
      460 416 39* 642 611 504 -> 2/20 stalls (matches 1/10 in P15). Both stalls identical: client contract set at t~0 while
      the provider had no platform transport; provider standby dial at +15 s; provider sets the return contract to our client
      only at ~+30 s, right after the client's 30 s evaluation window gave up ("[multi]receive no race and no client").
      The client's first sends and early resends fall into the offline window; if the resend that would land after the
      provider connects is scheduled past the 30 s window, the client drops the provider.
    startab.sh (A = b-keys 8845e7081eb2, StandbyDelay 15 s; B = b-keys-sb e17458b0f879 = same tree + URNETWORK_STANDBY_DELAY_MS=0;
      40 s idle, restart, client started at once, seconds to the first successful 1 MiB fetch through it, 60 s cap; n=10,
      alternating order, 11:14-11:34 UTC; probe cadence 2 s timeout + 0.3 s so values are quantized):
        A: 22.2 22.2 22.2 22.2 22.2 22.2 30.5 22.2 22.2 22.2   (median 22.2)
        B:  2.2  2.2  2.2  2.2  2.2  2.2  2.2  6.2  2.2  2.2   (median 2.2)
      10/10 paired, 20 s faster; no verification exits in either arm.
    => CONFIRMED: after every provider start on current code in a space without connect-v4/-v6 records, the provider is
       unreachable for the 15 s StandbyDelay, clients' first fetch waits ~22 s, and ~10% of clients that obtain a contract
       in that window drop the provider after their 30 s evaluation window. This is the P15 "second startup race".
    Fix options for the dev team (IPV6 A4 is their design): (1) provision connect-v4/-v6 DNS + certs (beta and production);
      (2) code: release the standby at once when no pinned transport can connect soon — e.g. every pinned dial fails name
      resolution (NXDOMAIN/NODATA) or is held (sleeping/idle-policy) — and keep the 15 s delay for slow/timeout failures;
      (3) apply StandbyDelay only after a pinned transport has connected once (the start case has nothing to wait for).

  P18. T17 FIX — RELEASE THE STANDBY WHEN NO PIN CAN CONNECT (branch fix/standby-unresolvable-pins, worktree connect-standby,
    base beta 216010c3; implemented by a single Fable agent, reviewed here).
    Change: a pinned PlatformTransport marks itself unresolvable when a dial attempt fails with *net.DNSError IsNotFound
      (existing authoritativeDnsMiss), cleared on success or any other error; the mark change notifies connectedMonitor; the
      group releases the standby before StandbyDelay iff every configured pin is Sleeping/IdlePolicy or Connecting+unresolvable.
      H1 needed a per-attempt observer in the dial ctx (net_http.go WsDialContextWithDialer): the strategy flattens a failed
      dial to "Timeout." after RequestTimeout, so the typed error only exists per attempt. H3 gets the typed resolve error.
    Tests (transport_family_test.go): ...ReleasesStandbyWhenPinsDoNotResolve (30 s delay, standby in ~20 ms),
      ...StandbyWaitsWhenPinsResolve (dead ports, nothing in 1.5 s), ...StandbyWaitsWithOneResolvablePin. Fixture
      newFamilyTestResolverOwning (NXDOMAIN for unowned names). Fail-before: test (a) fails "v4 pin never marked its hostname
      unresolvable". Mutations: every failure = unresolvable -> 3 fail; no wake -> (a) fails; any-pin instead of all -> mixed
      fails; coordinator re-check (release branch disabled) -> (a) fails "no connection arrived". gofmt/vet clean.
      Full package macOS ok 1094.8 s; Linux 6.1.0-9 (provider host, /opt/ur14) ok 1125.1 s.
    COMMITTED: a94ad7d0 on beta/custom-server (fast-forward from 216010c3), branch fix/standby-unresolvable-pins pushed.
    RIG A/B (startfix.sh, A = b-keys 8845e7081eb2, B = b-keys-fix e1353be76488 = /opt/ur14 = ur11 + patch; restart -> client ->
      first successful 1 MiB fetch; n=10 alternating; 11:49-12:09 UTC):
        A: 22.2 30.5 22.2 22.2 22.2 22.2 22.2 22.2 22.2 22.2   (median 22.2)
        B:  2.2  2.2  2.2  2.2 16.6 16.6  2.2  2.2  2.2  2.2   (median 2.2)
      10/10 paired faster; release line logged ~20 ms after every B start; verification exits 0 in both arms.
    RESIDUE (2/10 at 16.6 s): every B start logs two releases (two groups) and one H1 "[t]auth error = Timeout." ~12 ms later,
      identical in fast and slow runs. Agent code read: sdk deviceLocalProvider builds the group with provideMode zero, then the
      startup setProvideMode flips the public flag -> requestPlatformTransportMigration builds a second group and (when
      CanMakeBeforeBreakFrom is false) closes the first at once, cancelling its standby dial (the instant "Timeout.").
      Why the replacement is sometimes ~15 s late is NOT established (candidates: H1 budget reservation held until the old
      group's runners exit so the new pins/standby wait; relay-side replacement). Proposed sdk change (not ours to make blind):
      seed provideMode in the constructor so start-up is not a flip. Tracked as T18.

  P19. CEILING LEAD — LOSS RECOVERY IS EXPENSIVE: INJECTED-LOSS DOSE-RESPONSE (T19, pre-registered: +0.5% costing >=15%
    with a stable sign => the relay's ~0.5% loss is a material part of the ceiling; <5% => not the limiter).
    Injector: provider build urprovider.drop 8c8e8b2870dc (/opt/ur16 = beta + standby fix a94ad7d0 + key persistence +
      zz_dropdiag.go): URNETWORK_DIAG_DROP_PPM discards that share of >600-byte H1 websocket messages inside writeSendMessage
      (after the route accepted them) — the same position and shape as a relay drop. Same binary every arm; env only.
    Client urtun-clean-new c93cbabcd0af (kernel TUN, no diag), synth 8 flows 30 s, ceil2 harness, rotated order, zombies 0,
      13:33-13:49 UTC:
        +0      585 / 545 / 625 / 572   median 578   provider 0.80-0.89 cores, relay 1.30-1.43
        +0.5%   387 / 343 / 420 / 389   median 388   (-33%)   provider 0.53-0.62, relay 0.95-1.05
        +2%     158 / 154 / 138 / 152   median 153   (-74%)   provider 0.25-0.27, relay 0.55-0.64
    => Decision rule met by a wide margin (-33%, 4/4). Doubling total item loss (~0.5% relay + 0.5% injected) cuts throughput
       a third; CPU falls with throughput (nobody saturated), so the cost is protocol time, not work. Extrapolating to zero
       loss is NOT valid (other limits take over), but the slope says a lossless relay hop or cheaper recovery is the most
       promising ceiling lever found so far.
    Side fact: client RDIAG head-blocked time at baseline (ll6-r2, TUN f8) averaged 22.8% of wall time.
    Next: same arms on tdiag+xdiag+drop provider (urprovider.dropdiag 5a2efe37a63f, /opt/ur17) with urtun-x5 to see WHERE
      recovery costs time (sender window full, resend timing, receiver head-block, gap repair latency).

  P20. T19 MECHANISM (dropmech.sh; provider urprovider.dropdiag 5a2efe37a63f = b-diag4 tree /opt/ur17 + zz_dropdiag; client
    urtun-x5 d9b9106628e7 TDIAG; TUN f8 synth 30 s; two rotations 14:0x UTC). Medians of steady seconds:
                 goodput  noCap  rqAvg   resends/s  writes/s  client blocked  client past(dup)/s
      +0         619/586   69-71%  1320K   172-178   30.6-33.4k   19-22%          5-19
      +0.5%      447/403   78-80%  1480K   761-818   18.1-20.8k   78-80%          604-689
      +2%        157/149   92-93%  1540K   449-516    7.4-9.1k    (RDIAG absent)  -
    => One injected loss per ~200 items (~100/s) adds ~600 resends/s (~6 resends per loss), ~85% of them SPURIOUS (they arrive
       as already-delivered duplicates), and the receiver head is blocked ~80% of the time instead of ~20%. The window is
       full a bit more often (70 -> 79%) while writes drop 40%, i.e. items stay in the window longer: the ack clock slows.
       Every resend is "selective" recovery kind. Code read of the gap recovery / ack window running (agent).

  P21. T19 GERMANY + CODE READ + CANDIDATES.
    Germany (~100 ms) kernel-TUN urtun-m 91e12a92eb2f, 1 flow synth 30 s, provider dropdiag 5a2efe37a63f, alternating n=3:
      +0 134 / 133 / 133 (noCap ~95%) vs +0.5% 74 / 5(stall) / 71 (noCap 96-99%) -> -46%: loss costs at long RTT too.
    Code read (Fable Explore agent, file:line in transfer.go of beta):
      (1) Receiver writes selective acks one frame each in Go MAP order (writeSnapshot ~11020); sender snapshots acks between
          pack writes; scheduleSelectiveAckRecovery (~6279-6442) declares an item lost when >= SelectiveAckGapThreshold (3)
          selectively acked items with HIGHER sequence numbers are seen, burst 4/pass, no time grace on reliable-only paths
          (lateNotLost only when flightController.limited) -> neighbours of a real hole whose acks are still in the socket are
          "proven" lost -> ~6 resends per loss, mostly spurious. Resends are not backed off.
      (2) A selective ack re-Adds the item to the resend queue (receiveAck selective branch ~8707-8737) so its bytes still count
          against ResendQueueMaxByteCount; the window frees only on cumulative head progress. FLIGHTGATEFIX.md §19.7 names
          this starvation. Throughput = 2 MiB / cumulative-ack latency; a hole freezes it for ~20+ ms.
      (3) Ack compression (10 ms) + flushDeliver batching (64 frames or channel drain) delays cumulative progress per hole.
          Faster acks in general are DO-NOT-RETRY 17; not pursued.
      No window reduction / pause on TCP-only paths (reduceForLoss needs unreliable flights).
      Already rejected in FLIGHTGATEFIX (do not re-propose): larger send/receive window (§9), RTO floor change, logical
      lanes, ReliableAdmissionBoundedByDelivery (§22.4), ReliableLaneProvenRecovery (§26.2).
    Candidates built (diag, env-gated, never committed):
      S  client urtun-x6 db7a4288f3b1 (urtun-x5 tree + zz_sacksort.go): URNETWORK_DIAG_SACK_SORT=1 writes selective acks in
         ascending sequence order.
      R  provider urprovider.sackdiag 8898fee3a420 (/opt/ur18 = dropdiag tree + diagSackBytes): URNETWORK_DIAG_SACK_RELEASE_KIB
         lets up to that many selectively acked bytes stop counting against admission (TCP pipe accounting). 512 KiB keeps
         total in-flight <= 2.5 MiB = the receiver queue; 2048 KiB needs the receiver queue raised (RWIN 32).
    A/B sackab.sh (pre-registered: an arm is a candidate iff TUN f8 median >= +15% vs ctl with the same paired sign in 4/4 and
      resends not higher): arms ctl / S / R / SR / SR2, n=4 rotations.
    CAUTION: R/SR2 resemble "more window during holes"; DO-NOT-RETRY 16 (8 MiB constant window) gave nothing with resends 5x
      — mechanism (1) may be why, so S is the enabling test.

  P22. T19 CANDIDATE A/B RESULT (sackab.sh, 14:11-14:37 UTC, provider sackdiag 8898fee3a420, client urtun-x6 db7a4288f3b1,
    TUN f8 synth 30 s, TDIAG both ends in every arm, 5 arms rotated x4, zombies 0):
      arm  goodput             median  vs ctl paired %      resends/s        client blocked      dup(past)/s
      ctl  572 551 612 575     574     -                    147 229 171 143  18.9 34.1 19.0 20.6 15-19
      S    747 562 668 589     628 +9% +31 +2 +9 +2         103 173 118 178  12.4 27.5 19.2 23.4 0-1
      R    691 598 611 598     604 +5% +21 +9 0 +4          184 214 156 163  18.6 25.6 13.2 23.2 12-29
      SR   691 689 628 596     658 +15% +21 +25 +3 +4       132 109 140 161  14.1 11.4 18.2 21.4 0-3
      SR2  654 674 699 646     664 +16% +14 +22 +14 +12     164 165 119 188  11.2 9.3 6.2 12.8  3-10
    => S does exactly what the code read predicted (spurious duplicates 15-19/s -> ~0, resends -25%) but alone is +9%.
       SR2 (sorted acks + release up to 2 MiB of selectively acked bytes + receiver queue 32 MiB) meets the pre-registered
       rule: +16% median, 4/4 positive (12-22%), resends unchanged, head-blocked time halved. SR (release capped 512 KiB,
       stock receiver queue) +15% median but 2/4 small. n=4 only -> confirmation round next (SR2 vs ctl at base and at +0.5%
       injected loss; prediction: SR2 shrinks the injected-loss cost).

  P23. T19 CONFIRMATION (sackconf.sh 14:39-15:02 UTC, same binaries, alternating, n=4 per cell):
      base   ctl 625 584 562 681 (604) vs SR2 653 627 630 716 (642): paired +4 +7 +12 +5, median +6%
      +0.5%  ctl 447 389 399 396 (398) vs SR2 395 462 488 426 (444): paired -12 +19 +22 +8, median +12%
      pooled base with P22: 8/8 positive, paired median +12%.
      mechanism: base blocked 23 -> 13-14%, noCap 70-72 -> 64-66%, rqAvg 1.33 -> 1.43-1.49 MiB; at +0.5% resends 835 -> 366/s,
      in-flight 1.48 -> 2.78 MiB but blocked unchanged (80 -> 87%).
    => SR2 is a REAL but MODEST gain (~+6-12%, stable sign) and does NOT meet the pre-registered 15% rule; it does not remove
       the loss sensitivity (SR2 still -31% at +0.5%). Not shipped. Sorted acks (S) are the clean, receiver-only, wire-compatible
       part (kills spurious duplicates) — candidate for a small correctness/efficiency PR later, not a ceiling fix.
    NEW QUESTION: with network RTT ~0.3 ms, the 2 MiB window is still full 65-72% of the time => items take tens of ms from
      send to ack. Decompose send->ack latency (provider) vs arrival->ack-write hold (client) to split relay transit from
      receiver ack hold.

  P24. T19 LATENCY DECOMPOSITION + HOLE DURATIONS (provider urprovider.ldiag 0b49b5411cd9 = /opt/ur19 = sackdiag + zz_ldiag;
    clients urtun-x7 d4c492a567f9 / x8 946098ef82f9 / x9 70947b72a524; TUN synth 30 s; all diag env off except TDIAG).
    ld runs (f8 668/496, f1 620): provider send->cumulative-ack for first-send items mean 13.5-20 ms, median bucket 10-20 ms
      (f1 same). Client arrival->ack-queued median < 0.5 ms but p99 >= 100 ms; ack compress (first update -> write) mean 3-4 ms.
      => ~10 ms of the send->ack time is round trip through the relay path under load; the tail is items behind holes.
    Episode view (x8, queue non-empty stretches, hl-f8 615/582): stretches of 300-1000 ms cover 9-14% of wall time — but these
      are MERGED overlapping holes.
    Per-missing-head view (x9, hh-f8 562/617): ~120 holes/s < 10 ms (5% of wall time), ~18.5 holes/s 10-50 ms (mean ~11 ms,
      21% of wall time), 0.03/s 300-1000 ms (1%). => NOT long timer holes. Head-blocked time is dominated by ~18 holes/s that
      wait ~one AckCompressTimeout (10 ms) for the proof acks / head ack to be written.
    Candidate G (client urtun-x10 850b335db02e, URNETWORK_DIAG_GAP_ACK_NOW=1): end the compress wait at once when 3 selective
      acks are pending (gap provable) or a head ack lands while selective acks are pending (hole filled); at most one early
      write per snapshot. Sanity run gsan-G 666 Mb/s: 10-50 ms holes 18.5 -> 5.8/s, blocked 27 -> 14.6%, gapWrites ~100-170/s,
      BUT resends up (156-599/s vs ~170) — the DO-NOT-RETRY 17 side effect (faster partial acks + random ack order). Differs
      from row 17: only gap events are accelerated, steady ack rate unchanged.
    A/B gapab.sh running: ctl / G / SG (sorted + G) / SR2G, n=4 rotations; same pre-registered rule.

  P25. T19 GAP-ACK A/B RESULT (gapab.sh 15:20-15:42 UTC; provider ldiag 0b49b5411cd9; client urtun-x10 850b335db02e; TUN f8
    synth 30 s; TDIAG both ends all arms; zombies 0):
      arm   goodput              median  paired vs ctl %     noCap        resends/s          blocked %           dup/s
      ctl   650 591 591 583      591     -                   69-71        153 180 142 189    20.8 19.9 19.1 16.2 8-21
      G     725 739 719 639      722 +22% +12 +25 +22 +10    64-66        166 156 192 218    5.6 5.0 4.2 8.2     23-30
      SG    746 735 676 720      728 +23% +15 +24 +14 +23    64-65        129 111 159 168    5.8 4.2 8.0 5.6     3-8
      SR2G  746 653 675 665      670 +13% +15 +10 +14 +14    63-64        226 230 158 192    7.5 6.3 4.2 6.2     6-12
    => SG (receiver-side only: selective acks written in sequence order + compress wait ended when a gap becomes provable or a
       hole fills) MEETS the pre-registered rule: +23% median, 4/4 paired >= +14%, resends DOWN, duplicates ~gone, head-blocked
       time 20% -> 5%. G alone +22% but duplicates up (the row-17 side effect), which sorting removes. Window release (R) adds
       nothing on top once holes are short.
    Must still pass before any commit (row-17 lessons): confirmation n>=6 TUN f8, TUN f1, Germany ~100 ms (stall rate), socks.

  P26. T19 SG CONFIRMATION, SAME-DC (sgconf.sh 15:44-17:12 UTC... provider ldiag 0b49b5411cd9, client urtun-x10 850b335db02e):
    f8 (n=6 alternating): ctl 531 505 [181 startup stall] 608 566 549 | SG [176 startup stall] 669 689 716 668 727
      clean medians ctl 549 vs SG 689 (+25%); clean pairs r2 +32, r4 +18, r5 +18, r6 +32 (4/4).
    f1 (n=4): ctl 628 [240 startup stall] 715 669 | SG 748 748 [1 contract stall] 708 -> medians 669 vs 748 (+12%); clean pairs
      r1 +19, r4 +6.
    Stall classification (every run grepped): startup stall = "evaluation ping timeout"/"window_stall" in the client log (provider
      build is the OLD lineage: no standby fix, no key persistence) — ctl 2, SG 1. sg1-SG-r3 = NOT startup and NOT the ack path:
      warm-up fine, then the client's upstream send loop blocked 29 s with timer resends (TDIAG resends=152 selective=5, then no
      loop iterations 16:58:21-16:58:50) right after contract churn on the provider (contracts replaced every 1-3 s: 0.8 MB, 27 MB,
      54 MB, 107 MB "debit contract failed ... full") -> contract issuance stall (control plane, T4 class). Recorded, excluded.
      "[multi]receive race buffer limit reached" at +7 s appears in EVERY run (warm-up curl teardown) — not a signal.
    => SG confirmed on same-DC TUN: f8 +25% (8/8 clean pairs across P25+P26), f1 +12%. Next: rebuild the diag provider on the
       fixed lineage (standby fix + key persistence) so startup stalls stop polluting runs; Germany SG vs ctl with stall counts;
       socks client.

  P27. T19 SG AT LONG RTT + SOCKS (sgde.sh 16:15-16:51 UTC; provider urprovider.ldiag2 9ecba3cef9b7 = ldiag + standby fix +
    key persistence (/opt/ur20); Germany urtun-x10 850b335db02e; US socks ursocks-x10 4f17cdc2f0bb; alternating):
      Germany TUN f1 n=5 + f8 n=3: SG 133 133 135 133 128 128 126 129 (0/8 stalled) | ctl 4* 6* 129 129 29* 129 128 128 (3/8
        stalled/low, no startup-stall signature). Clean medians 129 vs 128.5 -> no throughput change at 100 ms (window-bound
        regime), and NO long-RTT stall regression (the row-17 concern) — if anything fewer (0/8 vs 3/8; n too small to claim).
      Same-DC socks f8 n=4: ctl 640 653 750 729 (691) vs SG 775 740 727 749 (745, +8%), paired +21 +13 -3 +3 (harness path).
    Production implementation (Fable agent, worktree connect-gapack, branch fix/receiver-gap-acks, uncommitted): transfer.go
      +104/-14 + transfer_receive_ack_gap_test.go; ReceiveBufferSettings.AckGapWakeSelectiveCount (default 3, 0 disables);
      ordered selective-ack writes with a reusable scratch slice; gap wake once per snapshot on >= 3 pending selective acks or a
      head advance while prior head < highest selectively acked seq (also covers selective acks written in an earlier snapshot).
      7 tests; fail-before 4 failing; mutations m1-m4 each caught; race x5 clean; full macOS suite ok 1102.6 s.
    Final production A/B next: clean provider b-keys-fix e1353be76488; clients urtun-gp-base 815c0906cabe (a94ad7d0 +
      winpatch relay-only switch) vs urtun-gp-fix a7750b6a35c6 (connect-gapack + same winpatch); TUN f8 n=6, f1 n=4.

  P28. T19 PRODUCTION-CODE A/B — PASSES (gpfinal.sh 16:53-17:20 UTC; provider b-keys-fix e1353be76488 clean (no diag) in all
    20 runs; clients urtun-gp-base 815c0906cabe x10 runs vs urtun-gp-fix a7750b6a35c6 x10 runs; ceil2 harness; zombies 0):
      TUN f8: base 584 579 594 662 671 607 (600.5) vs fix 717 701 684 830 779 694 (709)  +18%, paired +23 +21 +15 +25 +16 +14
      TUN f1: base 675 668 739 745 (707) vs fix 792 806 818 860 (812)                     +15%, paired +17 +21 +11 +15
      startup/eval stalls: 0 / 0.
    => Meets the pre-registered rule on the product-like path with production code and no diagnostics: 10/10 pairs >= +11%.
       Evidence chain: P19 (loss costs 33%) -> P20/P21 (mechanisms) -> P24 (per-hole durations: ~18 holes/s wait one 10 ms
       compress interval) -> P25/P26 (SG +23-25%) -> P27 (no long-RTT stall regression, socks +8%) -> P28 (production).
    Linux 6.1 full suite (/opt/ur21): only TestWebRtcFastPathCountsReceiveQueueDrop failed ("full fast receive queue did not
      record a drop"); PRE-EXISTING FLAKE: base tree /opt/ur14 fails it 2/5 (and a -count=10 batch), fix tree passes 5/5 in
      isolation; p2p fast-path queue counter, unrelated to acks.
    COMMITTED b7ec6b80 on beta/custom-server (ff from a94ad7d0); branch fix/receiver-gap-acks pushed.
    DO-NOT-RETRY 17 stays valid for STEADY faster acks; this change keeps the steady ack rate and only accelerates gap events.

  P29. T9 RE-BASELINE ON BETA b7ec6b80 (rebase.sh 18:40-19:00 UTC; provider urprovider.b7 5479b9ef4c52 = /opt/ur22 = b7ec6b80 +
    sn key persistence, no diag; client urtun-gp-fix a7750b6a35c6; TUN; n=3 rotated; eval stalls 0):
      synth f1 918 / 335* / 842   f4 837 758 774   f8 760 710 681   f16 691 809 686   CDN f8 748 731 643
      (*335: no stall signature, client 0.47 cores, contract churn mid-run; unexplained)
    => The shape INVERTED: 1 flow is now the fastest and throughput falls as flows are added (f1 ~880, f8 ~710, f16 ~690).
       Next limiter is per-flow cost inside the one sequence (candidate: T3 upstream ACK volume). CPU: client 1.25-1.52,
       provider 1.07-1.20, relay 1.80-1.89 cores, no thread > 50%.
  P30. PROVIDER-SIDE EFFECT OF b7ec6b80 + UPLOAD (provab.sh 19:01-19:31 UTC; providers b-keys-fix e1353be76488 (a94ad7d0 receive
    code) vs b7 5479b9ef4c52; client urtun-gp-fix; alternating n=4):
      TUN f8: old 667 696 677 659 vs new 649 675 721 659 -> no difference.
      TUN f1: old 82* 264* 121* 722 vs new 677 867 745 147* -> low runs (all hosts idle, no stall signature) old 3/4, new 1/4;
        not conclusive. In gpfinal (P28, earlier) the same old provider ran f1 792-860 with no low runs -> a single-flow
        mid-run stall class appeared on the rig this evening (relay host load average 3.4-4.4 at 20:22, other tenants).
      Upload socks (ursocks-m) to a same-DC sink on the client host, 20 s: new 261 215 541 226 vs old 130 237 167 436 ->
        too noisy to call; no regression visible.
    Capturing f1 stalls with full diag next (f1stall.sh: provider ldiag2, client x10 SG on).
    Dev team note (2026-09-14, via user): they are building a memory-budget-scaled transfer window ("more memory can get to
      gigabit"; root cause = transfer window + userspace NAT vs WireGuard's plain forwarding). Our angle: DO-NOT-RETRY 16 (8 MiB
      null, resends 5x) predates b7ec6b80; the resend blow-up is plausibly the random-order selective-ack mechanism, so a
      window sweep ON TOP of the fix is the useful complement.

  P31. WINDOW SWEEP ON TOP OF THE LOSS-RECOVERY FIX (winsg.sh 19:40-20:08 UTC; provider ldiag2 9ecba3cef9b7 with
    URNETWORK_WIN_MIB; client urtun-x10 850b335db02e, RWIN_MIB=32 in every arm, SG env on unless "off"; TUN synth 30 s; n=3
    rotated; eval stalls 0). f1stall.sh beforehand: 6 diag f1 runs 727-845, no stall reproduced.
      f8  2 MiB SG  618 639 776 (639)  noCap 65-68  inflight 1.34-1.38M  resends 131-244  blocked 5-11%   dup 4-8
      f8  4 MiB SG  616 520 613 (613)  noCap 51-59  inflight 3.08-3.12M  resends 518-573  blocked 24-38%  dup 22-42
      f8  8 MiB SG  495 548 499 (499)  noCap 46-50  inflight 4.98-5.01M  resends 640-835  blocked 40-48%  dup 63-100
      f8  8 MiB off 282 383 394 (383)  noCap 52-63  inflight 5.10-5.14M  resends 454-563  blocked 59-63%  dup 83-123
      f1  2 MiB SG  797 745 741 (745)  noCap 62-66  inflight 1.26-1.27M  resends 96-168   blocked 4-5%
      f1  4 MiB SG  322 152 425 (322)  noCap 0-6    inflight 2.13-2.77M  resends 154-263  blocked 7-10%
      f1  8 MiB SG  661 212 449 (449)  noCap 0      inflight 2.41-2.44M  resends 151-267  blocked 5-9%
    => On the same-DC relay path a bigger Transfer window LOSES throughput even with the loss-recovery fix (f8 -4%/-22%,
       f1 -57%/-40%); without the fix 8 MiB is -40%. DO-NOT-RETRY 16 stands after b7ec6b80.
    => Loss scales with in-flight: resends minus duplicates ~180/s (2 MiB) -> ~505 (4 MiB) -> ~635 (8 MiB). This is the
       strongest evidence yet that the relay drop (T2) is queue overflow driven by burst size (candidate: non-blocking 4096-msg
       forward queue, resident.go processClientForward).
    => f1 at 4-8 MiB: the window never binds and in-flight plateaus ~2.4 MiB, yet throughput halves -> a second inner limit
       (likely the provider's userspace TCP toward the client) degrades under the added delay/loss.
    Contrast T16 (Germany ~100 ms): 2 -> 4 MiB gave 1.6x. The window must be RTT-aware with a loss/delay response; scaling it by
       memory budget alone would regress relay-path users at low RTT. Sent to the dev team (response §13).

  P32. T3 REVISITED — UPSTREAM ACK VOLUME AFTER THE FIX. Client TDIAG/XDIAG from the P31 2 MiB SG runs:
      f1: client upstream sequence writes ~27.5k/s, ws writes ~28.4k/s, downstream data frames read ~37k/s (0.75 ACK/data)
      f8: upstream 39.5-47.2k/s, ws writes 39.9-49.7k/s, downstream data 31-36k/s (1.3 ACK/data)
    => At 8 flows upstream ACK messages exceed downstream data messages on the relay (one ws message per frame: the H1 frame
       grouping is not happening upstream). Relay queues are message-count bounded -> plausible cause of the flow-count slope.
    Why so many: the RIG's kernel-TUN client (scratch proxy/tunclient) writes each packet to /dev/net/tun individually (no
      IFF_VNET_HDR / GRO), so the client kernel ACKs every ~2 segments per flow. (connect/tun.go has gvisor GRO for WriteBatch;
      product platform tun paths differ — relevance to be stated carefully.)
    Diagnostic coalescer (urtun-x11 eb0ac0d67925, env TUN_READ_CHAN=1 [+ TUN_ACK_COALESCE=1]): reader -> channel -> sender;
      within each drained batch a pure IPv4 ACK (no payload, ACK flag only, no SACK/other options besides NOP/TS) is dropped
      when a newer pure ACK of the same flow with ack >= follows. Sanity f8 (provider b7, SG on): goodput 821, dropped ~70% of
      upstream packets (35-38k of 44-55k/s), no stall.
    A/B acoab.sh running: D direct / K channel only / C channel+coalesce; f8 n=4, f1 n=3; pre-registered rule as always
      (C vs K >= 15% median with stable sign -> ACK volume is a lever).

  P33. T3 ACK COALESCING RESULT (acoab.sh 20:15 + resume 22:29 UTC after a Mac low-memory kill of the harness; provider b7
    5479b9ef4c52; client urtun-x11 eb0ac0d67925; SG env on in all arms; TUN synth 30 s):
      f8 D direct  661 688 661 641 (661) | K channel 674 670 666 655 (668) | C coalesce 768 732 724 774 (750, +12% vs K,
         paired +14 +9 +9 +18), 77-82% of upstream packets dropped, relay cores slightly lower (1.31-1.41 vs 1.44-1.58)
      f1 D 751 761 714 (751) | K 811 772 762 (772) | C 0 852 435 -> WEDGE (ac1-C-r1: warm-up at full rate with 80% dropped,
         then the measured download's new connection moved 1.6 MB and stalled 28 s; no platform stall signature)
    => Upstream ACK volume is a real but moderate lever at 8 flows (+12%, 4/4 positive, below the 15% rule) and explains part
       of the flow-count slope. Dropping TCP ACKs is UNSAFE (single-flow wedge / halving) -> not a fix candidate; harness-only
       diag. The TCP-safe variant is fewer relay MESSAGES, not fewer packets: H1 grouping only drains already-queued Packs and
       never waits (transfer.go ready-drain loop: "it never waits to fill a batch, so a sparse request or TCP ACK keeps its
       original latency"), so upstream ACK frames go one per websocket message.
    Next (lingerab.sh, queued): client urtun-x12 1498ffc36154 with URNETWORK_DIAG_H1_GROUP_LINGER_US (wait up to N us per
      group slot for one more queued Pack on H1-only sequences); arms 0/250/1000 us; f8 n=4, f1 n=3; records upstream ws
      messages/s vs sequence writes/s and linger hits.

  P34. LINGER A/B WAS NULL BY CONSTRUCTION (stopped after 3 runs: 779/786/788, lingerHit 0/s in every arm). Policy counter
    build urtun-x13 023cedc7f3a5 (pol-san f8 796): the ready-drain group loop is entered ~1/s with h1Only=true, while upstream
    sequence writes are ~40k/s -> upstream TCP ACK packets reach the websocket through a different SendSequence branch that
    never groups. Code trace of that branch delegated (Explore agent) before any further experiment.

  P35. GERMANY PRODUCTION-CODE CHECK + UPSTREAM GROUP-MERGE LEAD.
    Germany (degp.sh 23:06-23:26 UTC; provider b7 5479b9ef4c52 clean; clients urtun-gp-base 815c0906cabe vs urtun-gp-fix
      a7750b6a35c6; TUN): f1 base 135 127 128 129 vs fix 128 92 127 129; f8 base 128 128 128 vs fix 126 128 128; stalls 0/0.
      => window-bound at 100 ms: no change, no stall regression with production code (one partial fix run, 92).
    Code trace (Explore agent): upstream IP packets -> sendPacket wraps each in a one-element parsedPacketGroup ->
      Client.sendGroupToWithTimeoutDetailed builds SendPack{logicalGroup: true} -> SendSequence.Run processPack handles
      logicalGroup via processLogicalGroupChunk and returns BEFORE the ready-drain group loop; processLogicalGroupChunk only
      chunks its own frames -> one wire Pack / TransferFrame / websocket message per upstream packet. transport.go
      writeReadySendBatch batches TLS writes but keeps websocket framing (net_websocket_batch.go). Relay work is per websocket
      message (NextReader, pool alloc, 4096-slot channel hops, ForwardWithTimeout per message).
    Diagnostic merge (Fable agent, scratch tree zz_groupmerge.go, env URNETWORK_DIAG_GROUP_MERGE=1; client urtun-x14
      84cfaec5f6c5): in the logicalGroup branch, when h1Only && !flowIsolation and the group is fully contained, drain
      already-queued whole logical groups (same Ack/ForceUnwrapped, <= 16 frames and the H1 byte limit, contractSafe as the
      ready-drain loop) into ONE sendRecordsForSchedulingKey item with ack/noAck record sets; no waiting. Tests: 64 queued
      single-frame groups -> 4 wire items, order and once-only callbacks asserted (gate off -> 64 items); existing
      LogicalGroup/H1Group/SendGroup/SendSequence tests pass gate on and off, -race x3.
    A/B mergeab.sh running: M0 vs M1, SG+TDIAG on both, provider b7; f8 n=4, f1 n=3; upstream ws msgs/s recorded.

  P36. GROUP-MERGE A/B RESULT (mergeab.sh 23:3x-00:4x UTC... provider b7 5479b9ef4c52; client urtun-x14 84cfaec5f6c5; SG+TDIAG):
      f8: M0 642 685 694 692 (689) vs M1 782 721 804 779 (781, +13%), paired +22 +5 +16 +13; upstream ws msgs 41-50k/s ->
          12.5-13k/s (~42-46k packs/s merged into ~4.8-5.4k items/s); relay cores similar or lower.
      f1: M0 780 731 746 vs M1 837 77* 262* -> single-flow SILENT STOPS again (as with ACK dropping, P33) although merge drops
          no packets.
    Anatomy of mg1-M1-r2 (client diag timeline): warm-up and first ~2 s of the measured download normal (~40k data frames/s,
      holes < 10 ms, 0 upstream resends), then BOTH directions zero for 27 s — no resends, no holes, no receive-queue block ->
      nothing sent by the provider. Provider log: 107 MB contracts replaced about once per second (00:37:00.5, 03.5, 04.4, 05.3
      BST, provider clock ~2-3 s ahead) and silence right after the 05.3 switch until a TLS handshake timeout line at 00:37:28.
      Same signature as sg1-SG-r3 (P26). The receive side trims (never rejects) beyond MaxOpenReceiveContract=4, so that cap is
      not the cause. Working hypothesis: a contract-switch stall whose probability rises with contract rate (throughput); it
      also hit unmodified clients this evening (single-flow low runs in P29/P30).
    => Merge is +13% at f8 (4/4, below the 15% rule on its own); f1 safety cannot be judged until the stall is explained.
    Capturing with provider-side sender diag: stallcap.sh (provider ldiag2, client x14 merge+SG, f1 x8, full logs kept).

  P37. DEV TEAM THROUGHPUT REPORT v2 (2026-09-14, "window rule shipped") — REVIEW + TEST SETUP.
    What they shipped on upstream main (connect 3c918bde; sn cca9b43a; sdk cb1e590): the delivery-sized transfer window rule ON by
      default (window = k x delivery over the round trip, clamped by a 1 Gb/s target, ceiling = attached pool / peer's advertised
      receive hold; SetWindowSizing(WindowSizingConstant) = one-call rollback); receiver window advertisement + committed-prefix
      (never-evict); carrier change voids selective acks (4533701); provider memory derived from host ((4/5 host)/(3 x count)
      target, process budget = soft limit; flow caps decoupled, c5f058bf); H3 share-table work on branches.
    Their evidence: in-process only, one layer (transfer window + hold), 200/400 ms, 2.4-4.0x (§3.23). §4: "No cell has used a
      native operating-system stack"; every platform row in §3.24 "derived, not measured". §8.3: on paths shorter than the 10 ms
      AckCompressTimeout the rule's growth factor g = 2 rtt_min/(rtt_min + c) falls below 1 and the window walks to its floor
      ("the short-path problem remains"). Their §2.1 "one flow reaches the ceiling" was pre-b7ec6b80 data.
    Overlap with ours: their §3.11c independently finds selective acks do not release items (our P21 mechanism 2); our P31
      (bigger window loses on the relay path because relay loss grows with in-flight) is the real-network counterpart of the
      regime their fixture cannot reach; our b7ec6b80 (gap-event ack wake + ordered selective acks) changes the c term they name.
    Merge test: worktree connect-merge, local branch test/merge-upstream-throughput 46978052 = beta b7ec6b80 + upstream/main;
      conflicts ip.go / ip_provider_unreachable_source_test.go / ip_upstream_tcp_buffer_linux_test.go / PROVIDERFIXES.md resolved
      to upstream (their reviewed replacements of our zombie release and rcvbuf fix); transfer.go auto-merged, our gap-ack and
      standby fixes intact; targeted tests (our gap-ack, standby group, their WindowSizing) pass. NOT pushed.
    Rig binaries: providers urprovider.up d1606c75172f (/opt/ur24: connect upstream/main + sdk upstream/main + sn main, go.sum
      updated with -mod=mod) and urprovider.mg d65d2f14e706 (/opt/ur25: same with merged connect); neither has our sn key
      persistence (sn main does not persist provide keys). Clients (scratch tunclient + winpatch relay-only switch + new
      HARNESS_MEMORY_BUDGET_MIB env calling connect.SetMemoryBudget before settings, to emulate a budgeted desktop process):
      urtun-up 968a8bd307bb, urtun-mg 480cef873da3. Beta arm: urprovider.b7 5479b9ef4c52 + urtun-gp-fix a7750b6a35c6.
    Sanity: UP stack same-DC TUN f8 = 259 Mb/s (beta ~710), provider 0.46 cores, no stall signature — consistent with §8.3.
    Matrix running (upmatrix.sh): arms BETA / UP (budget 384) / UPnb (no client budget) / MG (budget 384); same-DC f8+f1 n=4,
      then Germany f1+f8 n=3.

  P38. UPSTREAM REVIEW MATRIX RESULTS (upmatrix.sh; ceil2 harness; TUN synth 30 s; rotated arms; binaries as P37).
    Same-DC (~0.3 ms, relay path), n=4:
      f8  BETA 822 804 719 707 (762) | UP 304 340 261 293 (298) | UPnb 340 294 268 268 (281) | MG 312 293 274 278 (286)
      f1  BETA 883 833 455 845 (839) | UP 272 57* 237 289 (254) | UPnb 270 271 254 239 (262) | MG 273 250 223 (250)
      host CPU in upstream arms: client 0.35-0.57, provider 0.30-0.47, relay 0.63-0.95 cores (beta: 1.1-1.8) -> starved, not
      saturated. => every upstream-based stack is 62-70% SLOWER than beta on a low-latency path, in every run; our fixes merged
      on top do not recover it; the client's process budget makes no difference there.
    Germany (~100 ms), n=3:
      f1  BETA 131 128 1* (128) | UP 196 189 197 (196, +53%) | UPnb 6* 160 161 (160, +25%) | MG 203 203 198 (203, +59%)
      f8  BETA 127 127 16* (127) | UP 98 79 66 (79, -38%) | UPnb 152 155 156 (155, +22%) | MG 95 85 103 (95, -25%)
    => At 100 ms their rule does what it was designed for on ONE flow (+53%, +59% with our fixes); with a budgeted (desktop-like)
       client 8 flows LOSE 25-38%, while the unbudgeted client gains 22% -> a multi-flow interaction with the client-side
       budget/hold. Rollback test (window rule on vs SetWindowSizing(Constant) via env, same binaries up2/mg2) running to
       attribute the same-DC collapse.

  P39. ROLLBACK ATTRIBUTION, SAME-DC (rollback.sh; providers urprovider.up2 c00fcaca33a5 / urprovider.mg2 fb0afec7e5c9 cross-built
    on the Mac with zz_windowsizing_env.go (URNETWORK_WINDOW_SIZING=constant -> SetWindowSizing(WindowSizingConstant) at init);
    clients urtun-up2 4d678aa74831 / urtun-mg2 833aa9a1c5d5 same file; client budget 384 MiB; env on BOTH ends for "c"; n=3):
      f8  UPr 261 254 375 (261) -> UPc 421 521 466 (466, +79%) | MGr 234 273 270 (270) -> MGc 656 507 725 (656, +143%)
      f1  UPr 246 275 227 (246) -> UPc 513 616 593 (593, +141%) | MGr 230 251 259 (251) -> MGc 239* 772 879 (772, +208%)
    => The delivery-sized window rule IS the cause of the low-latency collapse (every run, same binaries, env only) — the
       §8.3 short-path defect, measured on a real relay path at ~0.3 ms network RTT.
    => With the rule rolled back, our fixes on top of upstream are still worth +41% (f8 466 -> 656) and +30% (f1 593 -> 772);
       MG-constant lands near beta (656 vs 762 f8; 772 vs 839 f1); remaining gap = other upstream changes or cgo-less build, open.

  P40. ROLLBACK ATTRIBUTION, GERMANY ~100 ms (rollbackde.sh; mg2 fb0afec7e5c9 + urtun-mg2 833aa9a1c5d5, budget 384; n=3 alternating):
      f1  MGr 198 196 204 (198) vs MGc 125 126 128 (126) -> the rule is worth +57% on one flow at 100 ms
      f8  MGr 39 117 80 (80) vs MGc 129 128 131 (129) -> the rule COSTS 38% on eight flows at 100 ms (budgeted client)
    => Rule verdict on real paths: helps a single flow at long RTT (their design point), hurts at low RTT (all flow counts,
       -58 to -67%) and hurts multi-flow even at 100 ms with a budgeted client. Decision report:
       ~/urnetwork-perf/UPSTREAM-WINDOW-RULE-REVIEW.md

  P41. USER DECISION (2026-09-15): beta takes option A (upstream merged, window rule default OFF, our fixes); PR upstream both
    option A (our fixes + rule default off) and an "aggressive" PR (A + production upstream group merge). PR A branch
    pr/option-a-fixes-rule-off (worktree connect-pra) = upstream/main 3a593216 + cherry-picks a94ad7d0 -> dd976e15 and b7ec6b80 ->
    1317530a (clean); default-off commit + test updates being written (agent). PR B branch pr/aggressive-upstream-group-merge
    (worktree connect-prb) from 1317530a; production group merge being written (agent).
  P42. SINGLE-FLOW SILENT STOP ROOT CAUSE (stallcap sc1-r2, provider ldiag2 diag + client x14 merge+SG, 425 Mb/s):
    client downstream arrivals normal to 00:50:14 then zero for 17 s; provider: 102.4 MiB contracts rotated every ~1 s
    ("debit contract failed ... 100% full" + "contract set" each second) at ~40-48k writes/s; after the 00:50:17-18 rotation the
    provider SendSequence loop printed NO TDIAG for 16 s (00:50:17 -> 00:50:34; the next line shows starve 94.7%) and ws writes
    fell to ~1/s -> the send loop was blocked in one call right after a contract rotation. => the silent stop is contract
    acquisition blocking the send loop when contracts rotate once per second at high throughput (StandardContractTransferByteCount
    128 MiB x ContractFillFraction). Not the merge or the ack changes: faster arms rotate more often and are exposed more.
    Test: provider urprovider.b7ct 02701d1a03ba (/opt/ur26 = b7 tree + zz_contractsize.go, URNETWORK_DIAG_CONTRACT_MIB); a 1024 MiB
    request is capped at 204.8 MiB effective (214748368 bytes: 256 MiB x 0.8), i.e. rotations halve. ctab.sh f1 n=8 alternating
    128 vs 1024 running.
    RESULT (f1, low run = < 400 Mb/s): c128 127* 828 720 853 837 735 857 (+r8) rotations 35-41/run; c1024 (eff. 204.8 MiB)
      759 231* 325* 88* 830 928 779 (+r8) rotations 19-20/run. Halving the rotation rate did NOT reduce the stalls (3/7 vs 1/7).
    => "contracts rotate once per second" is not a sufficient cause. The captured stall still began right after a rotation with
       the send loop blocked 16 s; open question is which call blocks (a specific slow contract create / verification /
       another lock), not the rotation frequency. Needs a goroutine dump of the provider during a stall. No contract-size change
       proposed.

  P43. SINGLE-FLOW SILENT STOP — CAUGHT AND EXPLAINED (catch.sh/catch2.sh: provider b7 clean, client urtun-gp-fix, f1; watchers
    dump goroutines + ss when NIC rate < 50 Mb/s for 4 s after > 300 Mb/s). cs1-r1 83 Mb/s, cs2-r1 289 Mb/s, both caught on the
    first run.
    Provider goroutines: every SendSequence parked in its normal idle select (transfer.go:7417 waiting packs/acks/timer, 6698 ack
    worker) — NOT blocked in a contract call (P42's "blocked in one call" reading withdrawn: the missing TDIAG lines were a
    loop with nothing to do).
    Client kernel socket at the stall (cs2-r1, ss -tinm): ESTAB, bytes_received 1.08 GB, lastrcv 4.5 s, r3629312 (3.6 MB queued
    unread) rb6291456, rcv_ooopack 2907, rcv_ssthresh 2096 -> a HOLE: one inner segment never arrived and everything after it is
    parked out of order; no retransmission ever comes. No tun write errors, no multi-client drop lines at the stall.
    Mechanism (code, beta b7ec6b80 transfer.go ReceiveSequence.receive): when the receive queue cannot fit an arrival it
    "removes later items to fit" — evicting items it ALREADY SELECTIVELY ACKED; the sender keeps selectively acked items on a
    60 s SelectiveAckTimeout and skips them in every resend path -> silent reneging (dev report §3.11c). At ~1 Gb/s on one flow the
    2.5 MiB hold fills in ~20 ms behind any hole. => the silent stops are reneging; upstream's never-evict / committed-prefix
    receive change targets exactly this (kept in option A). Contract rotation (P42) was coincidence.

  P44. SHIPPED PER USER DECISION (2026-09-15).
    Final rig matrix (prmatrix.sh; providers native up d1606c75172f, Mac-built pa 99de16bbe8d5 / pb 0b1b99a61d86, beta b7;
      clients urtun-up 968a8bd307bb, urtun-pa 62489e7802cd, urtun-pb c8c5ecde29c3, urtun-gp-fix; budget 384 on UP/PA/PB):
      same-DC f8  BETA 689 727 673 693 (691) | UP 324 267 310 264 (288) | PA 642 622 623 602 (622) | PB 725 737 718 710 (722)
      same-DC f1  BETA 804 807 871 382 (806) | UP 217 270 249 248 (248) | PA 193 730 91 302 (248) | PB 764 550 850 805 (784)
      Germany f1  BETA 134 130 132 | UP 2 204 197 | PA 130 127 131 | PB 127 17 50
      Germany f8  BETA 129 128 129 | UP 83 81 78 | PA 124 132 130 | PB 126 129 129
    Single-flow wedge caught on PA (catchpa pa1-r2 151 Mb/s): same hole signature (client socket r3451648, rcv_ooopack 2829,
      rcv_ssthresh 2096, lastrcv 4.5 s) -> not only the old eviction route; provider user TCP does not retransmit data segments
      (ip.go has SYN handling only), so any post-provider inner loss is permanent. Client kernel: TcpExtTCPBacklogDrop 4064
      cumulative, PruneCalled 3624, no softnet drops; bldrop.sh 8 healthy PA f1 runs 658-773 with 0 backlog drops -> cause of the
      missing segment NOT pinned. Open.
    beta/custom-server = 1e34c6fc (ff from b7ec6b80): upstream/main 3a593216 merged (conflicts in ip.go and its two tests and
      PROVIDERFIXES.md resolved to upstream) + default-off 1d8434af + THROUGHPUT-RIG-REVIEW.md. macOS full suite: only rule-behaviour
      tests failed (TestSizedWindowIsComputedFromTheMeasuredRoundTrip fails on clean upstream too; the other two pass 3/3 alone).
    Upstream PRs: #213 pr/option-a-fixes-rule-off b54f9f72 (standby dd976e15, gap acks 1317530a, default off 92e0ac3c, doc) and
      #214 pr/aggressive-upstream-group-merge 3bcd5aba (#213 + group merge; code identical to the rig-tested ef9a9e26).
      Local exp branch exp/upstream-sndbuf-uncommitted parks the old SO_SNDBUF experiment. Rig provider now urprovider.pa.

  P45. NEW GOAL (user, 2026-09-15): improve throughput from the ~100 ms Germany client; compare with WireGuard; then heavy latency and
    multiple hops. Reports REPORT-2026-09-15-UPSTREAM-WINDOW-RULE-AND-PRS.md (agents) and the "Window Rule on Real Paths" artifact
    (human) are with the dev team.
    Calibration of the Germany <-> provider path (Germany 1 vCPU, kernel 7.0, rmem_max 4 MiB; provider stock sysctls):
      ping 100.6 ms to both provider and relay; path MTU 1500 (1472 DF ok), WireGuard wg0 MTU 1420 (1392 DF ok inside).
      iperf3 download (provider -> Germany): plain TCP P1 256 Mb/s (3 retrans), P8 731 (534 retrans);
        through WireGuard P1 47 (1004 retrans), P8 59 (1405 retrans).
      raw UDP downloads (iperf3 -u -R, 1200 B): 50M 0.23% loss (mostly receiver socket), 100M 0.31%, 300M 10.1% (UdpRcvbufErrors
        +5,946 of 19,299 lost -> ~7% dropped on the PATH at 300 Mb/s). Germany NIC drops +0.
    => WireGuard (TCP-in-UDP) collapses on this path because the path drops UDP at high rates; urnetwork's TCP websocket (~130) is
       already ~2.5x WireGuard here. The meaningful ceiling is plain TCP over the same path: 256 (1 conn) / 731 (8 conns).
    Hypothesis H-G1: at 100 ms urnetwork is capped near ONE TCP connection's rate (the client's single websocket to the relay)
      before the transfer window. Test deg1.sh: A (pa, rule off) vs R (mg2, rule on) x f1/f8, websocket TCP_INFO sampled each second
      on the Germany host (cwnd, retrans, delivery_rate, rwnd_limited, rcv_space).

  P46. GERMANY DIAGNOSIS (deg1.sh sampler matched the wrong sockets — 13 tiny :443 connections, `ss | paste - -` misaligned;
    fixed as wssample.py: busiest established connection to relay:443 each second. deg2.sh; provider pa 99de16bbe8d5 / mg2
    fb0afec7e5c9; clients urtun-pa 62489e7802cd / urtun-mg2 833aa9a1c5d5 budget 384; TUN synth 30 s):
      A (rule off) f1: goodput 131, 2* | ws rx ~152 Mb/s, rcv_space 2.14 MB, rb 15.1 MB, unread 0, rcv_ooopack ~8k, client 0.48 cores
      A f8: 128, 125 | ws ~145-149, unread 0-31 KB
      R (rule on) f1: 196, 201 | ws ~228-233, rcv_space 4.4-5.4 MB, UNREAD 0.63-0.89 MB, client 0.63-0.65 cores (1 vCPU host)
      R f8: 67, 102 (deg1 same arm 191, 131) | ws 50-103, unread 0, client 0.33-0.36 cores
    => A is window-bound (2 MiB / 100 ms); the client websocket TCP is not the limiter (H-G1 refuted for A).
    => Rule-on single flow starts to load the Germany client (unread data in its socket, 1 vCPU) — a client-CPU ceiling near ~200-230
       on this host.
    => Rule-on 8 flows collapse on the SENDER side (client idle, nothing queued) and are unstable run to run -> window estimate
       behaviour with 8 inner flows and/or relay loss vs window. Next: provider build logging the rule's per-second window
       estimate/reason/in-flight/resends; constant 4 MiB arm.
    Deterministic simulation suite being built (agent, worktree connect-sim, branch sim/deterministic-path-scenarios; synctest).

  P47. DEV TEAM RESPONSE TO #213 (THROUGHPUTFIX-PR2.md, 2026-09-15; research plan, nothing implemented on their side).
    Verdict: evidence credible; default-off accepted as a temporary mitigation with an explicit trade (loses long-path single-flow
    gain AND, via ApplyWindowSizing, the shared send-budget attachment + receive advertisement in default-constructed settings).
    Their priorities: P0 wedge (H6) > P1 separate feedback vs target clamp (H1) > P1 relay loss + 8-flow reversal (H2/H3) > P1 ack
    wake bounds (H5) > P2 standby transitions (H7).
    KEY TESTABLE CLAIM (H1): at a Transfer R_min of ~0.3 ms the 1 Gb/s target cap = rate x R_min / 0.845 ~ 44 KB < the 256 KiB
      floor, so the rule window is the FLOOR on the same-DC path before any delivery feedback (below ~1.77 ms R_min). WDIAG on the
      provider (mg3 build) reports window/reason/TargetBound per second: chained h1-* runs after deg3 test this directly.
    H5 gaps in our gap-ack change (actionable): (1) once-per-snapshot wake is not once-per-interval — repeated triples can wake
      every snapshot with no elapsed-time bound; (2) hole-fill wake requires hasHeadAck: if Pack 0 is missing, the first cumulative
      head after selective acks does not wake early; (3) threshold counts len(selectiveAcks) including head-absorbed entries.
    H6: TUN write success != kernel acceptance; our rig client's per-packet tunFile.Write result is not evidence. Wedge loss site
      needs boundary discrimination (before Transfer delivery / during injection / after TUN acceptance / reverse path).
    They ask for: raw ledger, harness sources, config manifests; they note the agent report and ledger were not in the PR.
    Actions: (a) h1-* WDIAG runs + wedge runs with per-run kernel counters (after-deg3.sh, chained); (b) Fable agent: H5 fixes/tests
      on #213; (c) Fable agent: sanitized harness + reports into the PR branch; (d) sims agent already running (their §4 families
      overlap S1-S8).

