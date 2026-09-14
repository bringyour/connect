# ICMP in the ip* flow

Status: implemented (2026-07-31), uncommitted in the working tree. Scope:
echo only, permanently. Platform parity: the windows echo backend is
implemented (cross-compiled; on-device verification items open — see the
windows section). The client send gate (`MultiClientSettings.EnableIcmp`)
ships default off; flipping it is the rollout lever. See the decision log
at the end. Line references are against the tree at the design date and
will drift; symbol names are the durable anchors.

## Goal

Route ICMP through the multi client and egress it from the provider in
`ip.go` with a user-space ICMP socket, the same way TCP and UDP egress
through user-space sockets today. End effect: `ping` (ICMP echo, v4 and v6)
works end to end for users.

## Scope

The icmp path supports echo request/reply only. Decided 2026-07-31: this is
the permanent scope, not a first phase — there is no plan to extend to
other icmp types, in either direction. Non-echo icmp is dropped today and
keeps dropping, now deliberately.

The decision also matches the OS reality: unprivileged datagram ICMP
sockets — the only kind a provider running as a phone app or desktop
process can open — permit sending echo requests exclusively, on every
supported platform; arbitrary types would require raw sockets
(root / CAP_NET_RAW). Client-originated non-echo ICMP is also almost never
legitimate traffic.

Permanent non-goals:

- egressing non-echo ICMP types (no raw-socket path, on any provider)
- synthesizing ICMP errors toward the client (no port unreachable from udp
  `ECONNREFUSED`, no time exceeded for traceroute)
- IP fragment reassembly (fragmented pings larger than the mtu drop)

Platform parity is a requirement (decided 2026-07-31): every effort is made
to keep the app platforms at parity, so the windows echo backend
(`IcmpSendEcho2` family, evaluated feasible unprivileged — see the windows
section) is in scope. wasm remains excluded (no provider egress exists
there). Residual parity gaps, owned outside this change: hardened linux
distros without `ping_group_range` (deploy guidance below), and ICMPv6 from
apple devices (the apple tunnel carries no IPv6 routes at all).

## Current behavior

ICMP already reaches connect on every platform. The android `MainService`,
apple `PacketTunnelProvider` (which ignores the per-packet `protocols`
array), the windows `PacketPump`, and the sdk io loop
(`device_local_ioloop.go`) all forward tun packets unfiltered into
`DeviceLocal.SendPacket`. Everything then dies at one choke point,
`parseIpPathWithPayloadBorrowed` (`ip.go:4704`, "No support for protocol"),
reached at four gates:

1. client send: `RemoteUserNatMultiClient.SendPacket`
   (`ip_remote_multi_client.go:1845`) — sparse-logged, counted in
   `sendParseDropCount`. `TestMultiClientUnsupportedPacketLoggingIsSparse`
   (`ip_remote_multi_client_log_test.go`) pins this exact drop and must be
   rewritten.
2. provider ingress: `RemoteUserNatProvider.ClientReceive` (`ip.go:4236`) —
   silent skip.
3. provider dispatch: `LocalUserNat` `handleIpPacket` protocol switches
   (`ip.go:721`, `ip.go:791`) — `default:` drop, even for a parsed path.
4. client return: `multiClientChannel.clientReceive`
   (`ip_remote_multi_client.go:5573`) — fully silent drop, no counter.

The basic `RemoteUserNatClient` has the same two gates (`ip.go:4387` send,
`ip.go:4463` receive). The gvisor stack in `tun.go` registers
`icmp.NewProtocol4` but is inert for egress — it fronts the mux's local
DNS/DoH endpoint and the server proxy device, never the provider exit.

## Design

### Protocol model

One new enum value, `IpProtocolIcmp` (`ip.go`, next free ordinal), plus
`String()` → "icmp". The IP version already discriminates the wire protocol
number everywhere packets are built, so v4/v6 do not need separate enum
values; add `ipProtocolNumberIcmp4 = 1` and `ipProtocolNumberIcmp6 = 58`
beside the tcp/udp numbers.

`IpProtocol` appears in no protobuf and no wire frame — frames carry raw IP
packet bytes — so there is no wire, server, or SQL change anywhere in this
design.

### Parse and flow identity

Extend `parseIpPathWithPayloadBorrowed` with v4 protocol 1 and v6 next
header 58. The icmp header is 8 bytes: type, code, checksum, then
type-specific rest-of-header (identifier + sequence for echo). Parse rules:

- echo request (v4 type 8 / v6 type 128): `SourcePort` = identifier,
  `DestinationPort` = 0.
- echo reply (v4 type 0 / v6 type 129): `SourcePort` = 0,
  `DestinationPort` = identifier.
- any other type: parse error, exactly today's drop.

The identifier-as-port convention is what makes the rest of the system work
unmodified: `ReverseValue()` matches replies to the egress flow key, the
multi client `ip4PathUpdates`/`ip6PathUpdates` maps give each ping process
its own pinned window client, and `BufferId4`/`BufferId6` key the
provider-side nat table. `Protocol` is part of `Ip4Path`/`Ip6Path`, so icmp
flows cannot collide with a same-tuple udp flow.

Two invariants:

- the sequence number must never contribute to the path. `retainIpPath`
  keeps ports as the retained flow identity and
  `reconcileSendClientPath` (`ip_remote_multi_client.go:1352`) compares map
  entries by that key; a per-packet-varying port would unpin the flow from
  its client on every packet.
- fragments never parse. `parseIpv4` now drops any packet with mf set or a
  nonzero fragment offset (fixed 2026-07-31 for all protocols — see
  adjacent findings), so the icmp branch inherits the guard.

The borrowed parse stays allocation-free; the identifier is read in place
like a port. `payload` returns the echo data (`transport[8:]`) for
signature parity; no policy consumes it.

Hand-roll the header parse/build from RFC 792 / RFC 4443, matching the
existing hand-rolled tcp/udp parsers (gopacket remains test-only).

### sendShard

`sendShard` (`ip.go:597`) hashes `transport[0:4]` — ports for tcp/udp, but
type/code/checksum for icmp; the checksum varies per packet, so a flow would
split across shards when `SendShardCount > 1`. For icmp, hash the
identifier bytes (`transport[4:6]`) and nothing else from the transport.

### Security policy

One mandatory change and a verified list of safe defaults.

Mandatory — `cfaaDetector.inspect` (`ip_security_cfaa.go`): a port of 0 (or
any identifier value below 1024 on the ingress side, where "port" is the
identifier) falls into `port < 1024 → cfaaDrop`. Add an explicit icmp
branch that skips the port policy entirely while keeping the blocked-IP
reputation check — the blocklist must still apply to ping destinations.
This one function covers both directions (`inspectEgress` checks the
destination endpoint, `inspectIngress` the source endpoint).

Verified safe by existing `default:` branches, no change needed:

- `dmcaDetector.classify` / `touchEgress` / `touchIngress` — non-tcp/udp
  returns allow / no-op, so ping payloads cannot be entropy-dropped as
  unsanctioned-encrypted and create no flow-table state.
- `webStandardDetector.match` — false for non-tcp/udp.
- `isPublicUnicast` applies unchanged: no pinging RFC1918 / loopback /
  multicast / unspecified under a Public relationship (the smurf and
  LAN-probe guard). Network-relationship traffic bypasses, matching tcp/udp.
- `ipOosRst` — `(nil, false)` for non-tcp; all seven multi-client teardown
  call sites become silent no-ops for icmp flows, which is correct (a torn
  flow shows as ping loss).
- block actions and `ipAssoc` — destination-IP keyed, protocol-agnostic.
- contract accounting — `transfer_contract_stats.go` keys on
  `{contractId, receive}` only; billing cannot skew.

Hazard guard: `ipOosPacket` (`ip_packet.go`) panics on non-tcp/udp. All
callers are DNS responders gated on udp/tcp port 53, so icmp cannot reach it
today, but once icmp `IpPath`s exist the panic should become a safe nil
return.

### Multi client

Beyond the parse gate, three behavioral changes:

- `affinityIpPathsWithLock` (`ip_remote_multi_client.go:1257`): affinity
  resolves in priority order — base-domain buckets from the dns-observed
  reverse index (`serverNameLookup`, protocol-free), then per
  destination-ip+port for 80/53/443, then per-port for the rest of
  `< 1024`, then one version-wide bucket for user ports. Port-0 icmp would
  collapse into the `< 1024` branch: every raw-ip ping shares one bucket.
  Decided (revised 2026-07-31): icmp gets a per-destination-ip affinity
  case ahead of the port branches. A ping to a domain-resolved destination
  already joins the site's base-domain bucket via the first branch — `ping
  google.com` measures the same window client the site's real flows use —
  and the new case gives raw-ip pings per-host consistency instead of one
  shared bucket. Exemption would cost the same one case and forfeit the
  domain branch.
- `sendPacketDetailed` (`ip_remote_multi_client.go:4485`): add icmp to the
  udp case so it rides unacked under `UdpCollapsePrevention`. Ping is a
  measurement tool; unacked transfer lets tunnel loss show honestly as ping
  loss instead of being retransmitted away.
- `clientReceive` (`ip_remote_multi_client.go:5573`): the return-path parse
  failure is a fully silent drop. Add a sparse counter/log alongside the
  icmp case — it is the site that gates icmp replies reaching the client
  and today gives no signal at all.

Verified safe with no change: `canSendPacket` defaults to allow (the
tcp collapse-prevention gate is bypassed), `updateSequence` writes are
harmless and never read, the `removeClient` reset heap is tcp-gated (icmp
flows still detach via `update.client.Store(nil)`), and the per-channel
event-bucket source counts add one bounded entry per (destination, source
identifier) without perturbing `maxSourceCount` (its consumer filters to
port 443).

### Mux

`peekClaim` (`ip_mux_upgrade.go`): v4 icmp already classifies `peekOther`
and passes through unparsed. v6 next header 58 classifies `peekUndecided`,
which today pays one allocating failed `ParseIpPathWithPayload` per packet,
and after the parser change would parse successfully and fall through the
claim switch. Add `case 58` (and `case 1` on the v4 side for symmetry) →
`peekOther`. `IpMux.Receive` classifies by IP header addresses only and
needs nothing.

### Provider egress: IcmpBuffer / IcmpSequence

Mirror the udp shape: `Icmp4Buffer`/`Icmp6Buffer` wrapping a generic buffer
(flow table keyed by `BufferId4`/`BufferId6`, per-source `UserLimit` and
`GlobalLimit` with the `applyLruMapLimit` eviction, identity-checked
cleanup), each flow owning an `IcmpSequence` with a bounded `sendItems`
queue, one socket, a read goroutine, and idle reaping via `IdleCondition`.
Wire the two new buffers into `runSendShard` beside the udp/tcp four and add
the two dispatch cases in `handleIpPacket` (a new `parsedIcmp` view:
identifier, sequence, echo data; slices alias the packet per the existing
parsed views).

Only echo requests create egress flows: an outbound echo reply is always an
orphan (unsolicited inbound pings cannot reach a client, and datagram icmp
sockets cannot send type 0 anyway), so the buffer dispatch drops it
explicitly rather than leaving it to fail at the socket.

The sequence sits behind a small backend seam at echo-transaction
granularity — send an echo (ttl, seq, data), deliver replies — so the unix
datagram-socket backend and the windows `IcmpSendEcho2` engine share one
`IcmpSequence` (queue, idle reap, flow bookkeeping, return-packet
synthesis). Unsupported builds (wasm) stub creation and the flow drops
soft.

Socket, per flow:

- creation: unprivileged datagram icmp socket. Decided: `icmp.ListenPacket`
  from `golang.org/x/net` (already a dependency) creates the unix sockets;
  the icmp header parse/build stays hand-rolled per the clean-room and
  house parser conventions. This is a new creation point beside
  `DialContext` at `ip.go:1662` — the `ConnectSettings.DialContext` network
  whitelist (`net.go`) does not cover icmp networks, and stream dialing is
  the wrong shape anyway. The socket must get `applyEgress` (`egress.go`)
  applied, exactly as `egress_net.go` does for the pion sockets, so windows
  interface self-exclusion and future binding carry over. Mobile
  loop-protection is inherited from whatever excludes today's provider
  tcp/udp sockets from the device's own tun (app-level VPN exclusion);
  verify on-device, but no new plumbing is apparent.
- creation failure (EPERM, unsupported platform) fails soft: the flow drops,
  which is exactly today's behavior.

Identifier nat. The flow is keyed on the inner identifier; the wire
identifier is socket-scoped:

- linux: the kernel rewrites the outgoing echo id to the socket's ident and
  delivers only matching replies — demux is free.
- darwin: the id passes through but delivery to datagram icmp sockets is
  promiscuous, so the read loop must filter by the id it sent; v4 reads
  include the 20-byte IP header, which must be stripped.

Either way the sequence writes echo requests carrying the inner sequence
number and data, and on read rewrites id → inner id, rebuilds the return
packet toward the source (`writeIpv4Header`/`writeIpv6Header` with the
flow's addresses reversed, like `StreamState.udpPacket`), and recomputes the
checksum: plain RFC 1071 over the message for v4 (no pseudo-header — do not
reuse `transportChecksum`), pseudo-header with protocol 58 for v6
(`transportChecksum` is correct there). Sequence numbers and payload pass
through untouched, so RTT, loss, and dedup semantics survive end to end and
the client kernel's own id matching delivers the reply to the pinging
process. Note `StreamState.IpPath()` hardcodes `IpProtocolUdp`; the icmp
sequence needs its own state or a protocol field.

TTL passthrough (committed): copy the inner TTL / hop limit per write —
`ipv4.PacketConn.SetTTL` / `ipv6.PacketConn.SetHopLimit` on unix, the
`IP_OPTION_INFORMATION.Ttl` field on windows; one cheap syscall on a
low-rate path — so TTL-limited probes do not dishonestly reach the
destination. Time-exceeded responses never come back (error
synthesis is permanently out of scope), so traceroute shows timeouts until
the destination hop — honest, if unsatisfying.

Sizing: max echo payload within `DefaultMtu` 1440 is 1412 (v4) / 1392 (v6);
larger pings fragment at the tun and drop at the fragment guard. Flow cost
matches udp (one socket, two goroutines, bounded channels).

### Windows echo backend (IcmpSendEcho2)

Evaluated 2026-07-31: feasible, unprivileged, in scope (platform parity).

`iphlpapi.dll` provides `IcmpCreateFile`/`Icmp6CreateFile` and
`IcmpSendEcho2Ex`/`Icmp6SendEcho2` — the sanctioned unprivileged echo API
(no admin, no raw socket). Call via `golang.org/x/sys/windows` lazy procs
(x/sys is already a direct dependency; `egress_windows.go` is the
precedent), inside the service process where connect already runs (the cgo
export path used by `PacketPump`).

The API is a transaction, not a socket: one call per inner echo request
carries destination, payload, ttl (`IP_OPTION_INFORMATION.Ttl`), and a
timeout, and completes with a status, the responder address, and the reply
data. Consequences:

- the kernel assigns the wire identifier and sequence per call; the backend
  synthesizes the inner reply from the transaction result (type 0/129,
  inner id, inner seq, reply data), faithful whenever the status is
  success. The identifier nat is implicit.
- the engine issues calls asynchronously (`ApcRoutine`) from one dedicated
  os-locked goroutine that sleeps alertably; completions dispatch to the
  owning flow. One extra os thread total, regardless of flow count. A small
  per-flow outstanding cap (e.g. 4) bounds kernel-held reply buffers; over
  the cap new requests drop, which ping tolerates.
- parity of behavior, not maximal capability: only `IP_SUCCESS` with the
  responder equal to the flow destination synthesizes a reply. Every other
  status (unreachable, ttl expired, timeout) is silence, matching the unix
  backend — even though windows uniquely reports them.
- self-exclusion has no socket to bind, so pin the route by source address:
  resolve the egress interface (`EgressInterfaceIndex`, already maintained
  by the windows service's `EgressMonitor` → `setEgressInterfaceIndex`) to
  its unicast address (`GetUnicastIpAddressTable`) at flow creation and
  pass it as the call's source. Windows' strong-host routing then keeps
  echoes off the wintun adapter. `Icmp6SendEcho2` requires a source address
  in any case.

Fidelity deltas vs the unix backend, all cosmetic: duplicate replies are
absorbed by the kernel (`ping` never shows DUP! through a windows
provider), and the wire id/seq differ from the inner values (invisible end
to end).

Verify during implementation: `Icmp6SendEcho2` source-address semantics
(unspecified vs required concrete address), on-device confirmation that
source-pinned echoes bypass the wintun adapter while providing, and ci
loopback echo (works unprivileged on windows runners).

### Settings and memory

`IcmpBufferSettings` beside the udp/tcp settings: `ReadTimeout`,
`WriteTimeout`, `IdleTimeout` (60s — an echo flow is chatty or dead),
`SequenceBufferSize`, `UserLimit`, `GlobalLimit`, `OutstandingLimit`, mtu.
Defaults scale by the memory budget like the others, with small caps (echo
flows are one per ping process; floors well below the udp 256/512), pinned
in `ip_flow_limit_test.go` alongside the existing pins.

icmp is a specific item of the provider byte-cost model (decided
2026-07-31): `providerIcmpFlowByteCount` (8 KiB — the backend read/write
buffers, the send queue backing, flow bookkeeping, and the packet-class
pool buffer a live flow keeps in circulation) is the calibration lever, and
the provider-with-target profile derives the caps as
`natTarget/16 / providerIcmpFlowByteCount` with the functional floors
(64 per source / 128 aggregate). The constant is calibrated to measurement,
not assumed: an isolated run of `TestIcmpFlowMemoryFootprint` reports ~8.3
KiB marginal heap and ~8 KiB stack per live flow. Like its udp and tcp
siblings the figure is heap attributable — goroutine stacks and kernel
socket buffers sit outside it. The item is **additive above the udp/tcp
60/40 split**, not carved from it: an idle icmp path must not shrink the
udp/tcp tables, and since the caps are ceilings an unused table holds zero
bytes. While icmp flows exist, the worst-case overcommit of the nat target
is bounded by the divisor (~6%), the same shape as the existing functional
floors at small targets. `TestProviderIcmpSettings` pins the floor case,
the byte-derived case, that the derived caps stay inside the item's share,
and — for the additivity property — that the udp/tcp derivations are
byte-for-byte unchanged.

Budget tests, in the order they earn their keep:
`TestIcmpFlowBudgetModelCoversAllocations` is the deterministic CI guard —
it sums the per-flow allocations the settings imply (buffers, queue, pool
buffer, fixed structs) and fails when a settings change outgrows the model
the caps derive from, with no GC dependence. `TestIcmpBufferFlowLimits` and
`TestIcmpBufferIdleReap` are the enforcement side: caps evict by lru and an
idle flow reaps to zero. `TestIcmpBudgetItemIsAdditive` pins that
constructing a nat with a large icmp allowance costs nothing until a flow
exists (ceilings, not reservations). `TestIcmpFlowMemoryFootprint` is the
empirical marginal measurement; it always logs, but only asserts under
`CONNECT_MEMORY=1`, because heap figures in a shared test process inherit
the allocator and goroutine-teardown state of everything that ran before
and move ~3x with test order.

## Platform support (egress socket)

| platform | unprivileged echo | notes |
|---|---|---|
| android provider | yes, always | `ping_group_range` open to all apps |
| macOS / iOS provider | yes | strip v4 IP header on read; filter replies by id |
| linux provider | usually | systemd ≥239 opens `net.ipv4.ping_group_range` by default; hardened/older distros may not — verify the warp fleet, else one sysctl (or a raw-socket fallback under CAP_NET_RAW) |
| windows provider | yes — `IcmpSendEcho2` backend | unprivileged iphlpapi transaction API; source-address route pinning for self-exclusion (see the windows section) |
| wasm | no | build-tagged unsupported stub |

Client-side gap: the apple packet tunnel configures no IPv6 routes at all,
so ICMPv6 never enters the tunnel from apple devices regardless of this
change. The sdk return-path enum map defaults to `IpProtocolUnknown`
without dropping; extending the sdk enum is optional polish.

## Compatibility and rollout

Mixed fleet is the main risk. An upgraded client pinging through a
not-yet-upgraded provider gets a silent 100% blackhole for that flow (old
providers skip unparseable packets with no signal), and stickiness pins the
whole ping run there. There is no capability negotiation to hide behind.

Sequencing: land everything at once — the parse, policy, and egress sides
are inert until clients actually send icmp — and gate the client send path
(one flag on the multi client, default off) until provider saturation, then
flip the default. The flip is a release-default change in a following
release once the provider fleet broadly runs the parse/egress code — no
remote config. Provider-side-only deployment carries zero risk to existing
traffic.

## Abuse posture

Per-source and global LRU caps bound sockets and goroutines; contracts bill
the bytes; `isPublicUnicast` plus the cfaa IP blocklist constrain targets. A
ping sweep across a /24 is 254 short-lived flows — the same posture as the
existing tcp/udp scan surface, with the caps as backstop. No new detector.

## Observability

- `IpProtocol.String()` → "icmp" (trace tags and the multi-client incident
  log currently print "unknown").
- `SecurityDestination` rows appear as protocol icmp, port 0 — benign.
- new sparse counter for return-path parse drops (see multi client above).
- provider fanout/stats tests gain icmp rows where they enumerate
  protocols.

## Tests

Implemented in `ip_icmp_test.go` (the pre-implementation pins were flipped
to behavior tests): parse round-trips and the id-as-port reverse-matching
convention (v4/v6, gopacket-built), echo-only rejection (types, codes,
truncation, version/protocol mismatch), the orphan-reply and non-echo
dispatch drops, an end-to-end loopback echo through the LocalUserNat (real
kernel reply; id/seq/payload restored; skips without an unprivileged
socket), the reply builder with both checksum forms verified plus the
oversize drop, send-shard stability across sequence numbers, the cfaa
allow/blocklist branch, the dmca no-tracking default, `ipOosRst`/
`ipOosPacket` nil returns, mux `peekOther` in both families, the multi
client gate counters (disabled and enabled-through-policy), per-destination
affinity vs the port-0 collapse, and the provider/budget cap pins
(`ip_flow_limit_test.go`). Race mode clean on the new concurrency.

The original plan, for reference:

- parse/build round-trips for echo v4/v6, cross-checked with gopacket
  (`ip_packet_gopacket_test.go` precedent); truncated, fragmented, wrong
  types, checksum vectors from RFC 792 / RFC 4443.
- identifier nat: two sources pinging one destination with the same inner
  id → distinct flows, correct id restoration; reply demux under darwin
  promiscuous delivery (userspace filter).
- policy table: icmp bypasses the port policy both directions, blocklist
  still drops, private destinations → incident under Public, allowed under
  Network.
- multi client: flow stickiness by identifier, affinity bucketing, unacked
  send option, return-path counter; rewrite
  `TestMultiClientUnsupportedPacketLoggingIsSparse` for a genuinely
  unsupported protocol (e.g. 47) and add the icmp-accepted counterpart.
- end-to-end: client nat ↔ provider nat echo over the test harness, mirror
  of the udp integration tests; memory pins in `ip_flow_limit_test.go`.
- socket tests skip gracefully on EPERM (unprivileged CI without
  `ping_group_range`); they run unprivileged on macOS dev machines and
  systemd CI runners.

## Performance

Measured 2026-07-31 by benchmarking a worktree at the pre-icmp commit
against the implementation, interleaving the two runs sample by sample so
thermal drift cancels, with microbenchmarks on each per-packet function the
change touches (`parseIpv4`, the borrowed path parse for tcp4/udp4/tcp6,
`sendShard`, `cfaaDetector.inspect`, `peekClaim`) plus the end-to-end
`BenchmarkIpEgress*` set. `parseIpv6` serves as the untouched control. Two
methodology notes that materially changed the answer: the microbenchmarks
must consume every result or dead code elimination measures the elimination
rather than the code (an un-sinked `parseIpv4` reads 0.84 ns), and a single
non-interleaved pass reported ±30-44% variance that swamped the real
effects.

The first pass found two genuine regressions, both fixed:

- `parseIpv4` +32%: the fragment guard read two bytes separately. Now one
  16 bit load and mask (`Uint16(ipPacket[6:8])&0x3fff`), which is also the
  clearer statement of "mf or any offset".
- the borrowed path parse for **udp** +24%, more than the guard alone
  explains: adding icmp cases to the protocol switch perturbed the compiler's
  dispatch for the hot tcp/udp cases. The icmp parse moved out of line into
  `parseIcmpIpPathBorrowed` (`//go:noinline`, called from `default:`), which
  restores the original two-case shape.

After the fixes every path parse, `sendShard`, `cfaaDetector.inspect`, and
`peekClaim` measure flat against baseline, allocations are unchanged
(zero on all of these), and no end-to-end egress benchmark regresses.

One deliberate cost remains: `parseIpv4` is ~0.5 ns slower than pre-icmp
(+22%), the irreducible load/mask/branch of the fragment guard. That is the
adjacent correctness fix rather than icmp overhead — it is what stops a
continuation fragment from being parsed as a transport header — and it is
~2.5% of a full path parse and negligible against the microsecond-scale
packet path. Revisit only if a profile ever shows the ipv4 header parse
mattering at that granularity.

## Adjacent findings (independent of icmp)

Fixed in the working tree, 2026-07-31:

- fragment parsing hole: `parseIpv4` ignored the fragment offset, so a
  non-first fragment of a large udp datagram parsed payload bytes as ports
  and could dial a garbage flow. Fixed for all protocols in `parseIpv4`
  (mf or a nonzero offset fails the parse), pinned by
  `TestParseIpPathIpv4FragmentDrop`.
- `newParsedPacket` (`ip_remote_multi_client.go`) had zero callers —
  deleted (the `parsedPacket` type stays; the send paths use it).
- `multiClientChannel.ingressSecurityPolicy` was assigned but never invoked
  (all ingress inspection happens in `clientReceivePacket`) — the field and
  its window/channel plumbing are removed.

Also landed with the icmp change: the formerly silent return-path parse
drop now has a per-channel sparse counter and power-of-two log summaries
(`multiClientChannel.receiveParseDropCount`).

## Later directions (echo-scoped only)

Type extension is decided against (see Scope); the remaining direction
stays within echo:

- icmp-aware blackhole detection in the window (re-route a ping flow that
  gets zero replies), if mixed-fleet pain shows up in practice.

## Decision log

- 2026-07-31: echo request/reply only, permanently, with silent drop of all
  other types in both directions. The raw-socket privileged path and icmp
  error synthesis (port unreachable, time exceeded) are dropped from
  planning, not deferred.
- 2026-07-31: platform parity is a requirement ("every effort to keep the
  app platforms at parity"). The windows `IcmpSendEcho2` backend is
  evaluated feasible unprivileged and moved into scope.
- 2026-07-31: locked the open defaults — icmp affinity is per destination
  ip (revised same day from an initial exemption: the protocol-free
  base-domain affinity branch makes affinity both cheap and diagnostically
  truthful, so a domain-resolved ping rides the same client as the site's
  flows); outbound echo replies drop explicitly at the buffer dispatch;
  ttl passthrough is committed; the unix socket layer uses
  `golang.org/x/net/icmp` with hand-rolled headers; the client send gate
  ships default-off and flips as a release default after provider fleet
  saturation.
- 2026-07-31: adjacent findings fixed in the working tree (fragment guard,
  `newParsedPacket`, dead `ingressSecurityPolicy` plumbing) and baseline
  behavior pins added (`ip_icmp_test.go`).
- 2026-07-31: icmp became a specific item of the provider memory budget:
  `providerIcmpFlowByteCount` with target-derived caps (`natTarget/16`,
  floors 64/128), additive above the udp/tcp split so an idle icmp path
  never reduces the other tables (caps are ceilings; unused holds zero
  bytes). Replaces the earlier fixed-floor caps. Writing the budget tests
  then corrected the constant from an assumed 4 KiB to a measured 8 KiB,
  and surfaced two real defects fixed in the same pass: the backend read
  and write buffers were a flat 2048 rather than mtu-derived, and the
  sequence send loop armed a `time.After` per iteration instead of reusing
  one timer (the CODESTYLE hot-path rule the udp sequence already follows).
- 2026-07-31: benchmarked against a pre-icmp worktree (see Performance) and
  fixed the two regressions it found — a two-byte fragment guard became one
  16 bit load, and the icmp parse moved out of the hot protocol switch into
  a noinline cold tail after the added cases measurably slowed the udp
  parse. Post-fix the only remaining delta is the fragment guard's own
  ~0.5 ns, which is the correctness fix, not icmp.
- 2026-07-31: implementation landed (uncommitted). Notable in-flight
  choices: `IpProtocolIcmp = 3` single enum; the icmp settings drop the udp
  `WriteBatchSize`/receive-shard machinery (echo is low-rate; writes are
  single-datagram and replies deliver synchronously from the read loop);
  version/protocol mismatch keeps the "No support for protocol" error while
  non-echo shapes fail as "Unsupported or malformed icmp packet";
  `deliverDownstream` gained a nil-packet guard backing the `ipOosPacket`
  nil return; the windows engine uses `QueueUserAPC` + alertable `SleepEx`
  with the v6 reply parsed at pack(1)-derived offsets (on-device
  verification item). Cross-compiles verified for windows amd64/arm64,
  wasm, linux arm64, android arm64. The unsupported-protocol log pin was
  rewritten against gre (protocol 47).
