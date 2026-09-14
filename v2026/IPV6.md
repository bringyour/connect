# IPv6 plan

Status: design decisions locked 2026-09-11 (section 2 onward). The original
proposal is kept verbatim below; two of its bullets are superseded and marked.

## 1. Original proposal

- providers connect to both connect-v4.bringyour.com and connect-v6.bringyour.com, so they maintain two active transports . For any network space connect url, X.Y, the v4 and v6 variations can be made with X-v4.Y and X-v6.Y . The network space should just support query of the v4 and v6 urls derived from the platform/api urls using the convention.
- connections in server/connect client connections are tagged with v4 or v6 . Clients are said to support v4/v6 only if they have an active connection on that protocol. This is proof by work.
- findproviders2 can accept an explicit dualstack (v6 and v6), v4-only, and v6-only filter, and the providers need to be categorized by both, v4-only, and v6-only in the stored provider ranks
- the multi client maintains an explicit dualstack window, v4-only window, and v6 only window . ipv6 traffic goes to the dualstack window, falls back to v6 only. Ipv4 Traffic goes to the dualstack window if it has providers, otherwise it falls back to the v4 only window. **Superseded by B1 and B3: per-family minimums inside the existing windows, and capability-based routing.**
- The window monitor exposes whether each provider is dualstack, v4-only, or v6-only
- In the connect drawer, we need to add a dualstack/v4/v6 histogram that shows the provider dots stacked horizontally under either both, v4, or v6. This should appear under the transport type display. The dots should be the same size as on the connect widget, and the horizontal rows should wrap if there are more dots than fit on a row. The ip version histogram needs to be added to all apps: android, apple, windows, linux, and mmm/ur.io .
- the provider details should show dualstack, v4, or v6 for all provider rows
- the tunnels will need to accept v6 :: and route it the same as v4 
- the dns doh needs to resolve both A and AAAA records. The doh connections need to race both IPv4 and IPv6 using happy eyeballs. 
- the server/proxy needs to use dualstack happy eyeballs to race IPv4 and IPv6. Wherever the code originates a TCP connection (e.g. http client) it should use happy eyeballs to race v4 and v6.

All connect and server/connect tests need to be extended to be dual stack. The connect code needs to be audited for any IPv4 packet parsing and make sure that there is an IPv6 case also.

## 2. What already exists

- The NAT core in `ip.go`, `ip_packet.go`, `ip_icmp.go`, `ip_sni.go`, the
  security tables and the blocker are already dual-stack. Provider egress
  dials use `"tcp"`/`"udp"` with the inner packet's literal, so a v6
  destination gets a v6 socket wherever the host has one.
- Windows are keyed by `WindowType` with per-type `WindowSizeSettings`, and
  the quality window already carries a per-attribute minimum
  (`WindowSizeMinP2pOnly`) that raises the target and steers admission. That
  is the precedent for the family minimums in B1.
- `connect.bringyour.com` and `api.bringyour.com` have A and AAAA records.
  `api-v4`/`api-v6.bringyour.com` exist, are single-family, and have nginx
  blocks, cert SANs and `expose_aliases`. `connect-v4`/`connect-v6` do not
  exist at any layer.
- The edge nginx listens on `[::]`, the server accepts v6 clients
  (`AllowOnlyIpv4 = false`), and `session.ResolveClientAddress` yields a
  `netip.Addr`. Nothing stores the family.
- `control_family.go` holds the control-plane policy (Auto/Force4/Force6)
  and the demotion ledger. `net_http_internal_doh.go` has the only real
  happy-eyeballs dialer, scoped to network-space names.

## 3. Decisions

### A. Provider transports and server tagging

A1. Only the provider role runs two platform transports. Consumer channels
(`ApiMultiClientGenerator.createPlatformTransport`) and the api client stay
single and family-agnostic, dialing `connect.bringyour.com` with happy
eyeballs.

A2. Family-pinned transports. `PlatformTransportSettings.IpFamily` (0, 4, 6).
A pinned transport dials its family URL (`connect-v4.` / `connect-v6.`) with
the network narrowed to `tcp4`/`udp4` or `tcp6`/`udp6` regardless of policy
or demotion, and resolves only the matching record type. Hostname steering
alone is not trusted.

A3. Intent and proof. A pinned transport declares its family: header
`X-UR-IpFamily: 4|6` on H1, and a new `ip_family` field on `protocol.Auth`
for H1 v1 and H3. The server records the observed remote family from
`X-UR-Forwarded-For` (H1) or the PROXY-protocol remote (QUIC), unmapped. A
connection proves a family only when intent equals observation. No intent is
legacy and counts as v4 (today's assumption). A mismatch proves nothing and
is logged. The hostnames are a client-side steering device; the server has no
host logic.

A4. Direct only, plus a legacy standby. Pinned transports use direct dialers
only (no extender, no SOCKS). A third, family-agnostic standby transport with
the full dialer set and no intent runs only while neither pinned transport is
connected, after a `StandbyDelay` (default 15s) from start or from the last
pinned disconnect, and stops when a pinned transport connects. This covers
unprovisioned names, blocked DNS and censored networks, where the provider is
then tagged legacy v4. `FamilyPlatformTransportGroup` (transport_family.go)
owns the three transports; the SDK exposes their states through
`Device.GetProviderFamilyTransportStatus`.

A5. Sleep instead of spin. A pinned transport sleeps while
`probeFamilySupport(family)` reports no global address of its family and
wakes on `AddNetworkChangeListener`. Its dial failures never call
`noteBackendFailure`, and it uses its own jittered backoff rather than a
shared `NextConnectTime` pacing step. Implementation check: the probe's
reserved-range list must not exclude the 464XLAT CLAT range (192.0.0.0/29),
or v4 over NAT64 hosts is never attempted.

A6. Policy. `IpFamilyForce4`/`Force6` conflicts with the opposite pinned
transport; `controlDialNetwork` already errors on that, so the transport idles
and the provider advertises one family. Demotions leave explicit families
alone, so pinned transports bypass the ledger while still feeding it.

A7. Budget and modes. Both pinned transports register at
`PlatformTransportBudgetPriorityBackground`; the budget already lets H1
precede optional H3, so the second degrades to H1 under pressure. Verify
`MaxTransportCount` admits the extra provider transport. The QUIC socket in
`transport.go` binds `udp4`/`udp6` per family instead of `IPv4zero`. The v6
transport excludes `h3dnspump` (`whodis.bringyour.com` has no AAAA).
`migratePlatformTransportWithPolicy` replaces both pinned transports.

A8. Server storage and categorization.

- `network_client_connection`: `ip_version smallint` (observed 4/6, 0 for
  pre-migration rows) and `ip_family_intent smallint` (0 legacy, 4, 6).
- Aggregation in `UpdateClientLocationReliabilitiesInTx`: `ipv4_proven` =
  any connected row with intent 0, or intent 4 and observed 4; `ipv6_proven`
  = any connected row with intent 6 and observed 6.
- Validity fix: `client_address_hash_count` becomes the max per-family
  distinct hash count, and the location is taken from v4 rows when any exist,
  else v6 rows. The generated `valid` expression is unchanged. The probed
  egress location keeps overriding both when fresh.
- `ClientScore.IpFamilies` bitmask (1 = v4, 2 = v6). Gob zero value means
  legacy and is treated as v4-only.
- Rank cache: a family facet on the sample buckets at build time (three
  facets per key). Capable filters draw the dualstack facet first, then the
  single-family facet. Exact filters read one facet. Readers fall back to the
  un-faceted keys until one complete faceted export has set
  `client_score_ip_family_v1_ready`, so deploy the api before the taskworker.
- Category per provider: `dualstack`, `v4-only`, `v6-only`.

A9. Infra. Add to `vault/main/services.yml` under `connect`, mirroring the
api list: `connect-v4`, `connect-v6`, `main-connect-v4`, `main-connect-v6`
for both `bringyour.com` and `ur.network`. Publish A-only and AAAA-only
records for the two families pointing at the existing connect edge addresses.
Regenerate certs and nginx via warpctl. `sdk.NetworkSpace` gains
`GetPlatformUrlV4/V6` and `GetApiUrlV4/V6`, inserting the suffix on the
service label so `g2-connect` becomes `g2-connect-v4`; explicit override URLs
and IP literals return no family URL and disable pinned transports.

### B. Discovery and routing

B1. No new windows. Each window tracks the category of every exit
(`DestinationStats.IpFamily`, from find-providers2) and holds two minimums:

- v4-capable exits: the existing hard `WindowSizeMin`. Dualstack and v4-only
  count.
- v6-capable exits: `WindowSizeMinIpv6Capable`, soft, default 1 for quality
  and 0 for speed. Dualstack and v6-only count. Soft means it raises the
  target like `WindowSizeMinP2pOnly` but never changes `windowSizeMin`, so it
  cannot make the window read as unsatisfied. Fixed-size and fixed-destination
  windows are exempt.

B2. Fill. The main fill asks the generator for `v4-capable`; the server draws
dualstack first, so v4-only exits only top up when dualstack runs out. When a
window has zero v6-capable exits and a v6 shortfall, it issues one extra
request for `v6-capable`, which returns dualstack if any exists and v6-only
otherwise. The expand plan prefers candidates that close the open shortfall,
exactly as it prefers p2p-only today. A category the server returned nothing
for is marked starved and re-polled after `IpFamilyStarvedRetryTimeout`
(default 60s), not on `FormationPollTimeout`. When over target, collapse
sheds single-family exits before dualstack ones.

B3. Routing. `orderedClients` takes the packet's IP version and skips exits
whose category cannot carry it. v4 flows use any v4-capable exit, v6 flows
any v6-capable exit, with today's weights and window order. No preference
bonus for dualstack on v4 flows.

B4. Generator interface. Add optional `MultiClientGeneratorWithIpFamily`
with `NextDestinationsWithIpFamily(count, exclude, rankMode, ipFamily)`.
`ApiMultiClientGenerator` implements it. A generator without it is called as
today and its destinations are treated as legacy v4-only.

B5. Local downgrade. Per-exit dial-failure counters are split by version.
When the v6 counter reaches the existing dial-starvation threshold while v4
stays healthy, the exit is downgraded to v4-only for the life of the window.
That may open a v6 shortfall and trigger a `v6-capable` fill. Qualification
probing follows the category: v4-capable exits probe over v4, v6-only exits
over v6, and dualstack exits alternate families across passes; a dualstack
exit is proven by either family's pass.

B6. No v6-capable exit. `RemoteUserNatMultiClient.Ipv6Available()` is true
when any window has an added v6-capable exit and is exported on the window
status for the apps. The corrective actions key on the narrower
`Ipv6Unroutable()`: the windows have formed and none carries v6. While that
holds, the in-tunnel DNS interceptor in `ip_mux_upgrade.go` answers AAAA with
an empty NOERROR without an upstream query (`SetIpv6Unroutable`), and a v6
packet with no candidate gets an ICMPv6 destination-unreachable no-route
reply so connects fail fast. Neither fires while the windows are still
forming, which would push every dual-stack app onto v4 for the session.

### C. Data plane and DNS

C1. gVisor tun (`tun.go`). Register `ipv6.NewProtocol` and
`icmp.NewProtocol6`; add a `LocalIpv6AddressAllocator` over a fixed ULA /64
mirroring the v4 allocator; add the `::/0` route; make `write`,
`WriteBatch`, `tcpInboundFlow` and the endpoint lookup dual-stack; make
`Tun.dialContext` accept `tcp6`/`udp6` and v6 literals and resolve A and AAAA
with the shared happy-eyeballs dial. The tunnel interface mtu is a separate
constant, `DefaultTunnelMtu = 1280`: Linux, Darwin and gVisor refuse IPv6 on a
link below RFC 8200's minimum, but IPv6 does not require packets to be that
large, and the packet-size contract `DefaultMtu = 1100` is what keeps a full
return packet inside one optimistic H3 DATAGRAM. Raising the packet size to
1280 would have moved every full-size H3 packet onto the reliable stream on
every real path. The tun, the native tunnels and the sdk mtu getter use
`DefaultTunnelMtu`; the packetizer keeps `DefaultMtu` for both families.

C2. Platform tuns. Android `MainService.kt`: `allowFamily(AF_INET6)`, a v6
tunnel address, `::/0`, excluded routes for `fe80::/10`, `fc00::/7`,
`ff00::/8`, `::1/128` on T+ and a generated split table before T; escape mode
keeps today's behavior and does not capture v6. Apple
`PacketTunnelProvider.swift`: `NEIPv6Settings` with the default route and the
same exclusions, which also closes the current v6 bypass on dual-stack
networks. Windows `NetworkConfig.h` and Linux `Tunnel.hpp`: v6 address and
default route. SDK: default IPv6 tunnel DNS addresses.

C3. Extension headers and fragments. One shared v6 extension-header walker
(hop-by-hop, routing, destination options, fragment) used by `parseIpv6`, the
SNI peek, the mux `peekClaim`, the first-load peek and the `sendShard` hash.
Fragment-header reassembly on ingress mirroring `ipv4Fragments`, fragment
emission for oversized v6 UDP mirroring `fragmentIpv4Packet`, and the
`Version != 4` fragment gates in `ip.go` and `ip_remote_multi_client.go`
generalized.

C4. ICMPv6 and egress hygiene. Handle Packet Too Big for path MTU and
Destination Unreachable; drop NS/NA/RS/RA silently. Before policy checks on
egress, unmap v4-mapped addresses and block 6to4 (`2002::/16`), Teredo
(`2001::/32`), the NAT64 well-known prefix (`64:ff9b::/96`), ULA, link-local,
multicast, loopback and unspecified, so a v6 packet cannot reach private v4
space.

C5. DoH. Populate `RemoteDohUrlsIpv6`, `RemoteDnsIpv6` and `LocalDnsIpv6`
with the v6 endpoints of the existing operators; `DefaultDohSettings().
IpVersion = 0`; add `Ipv6` to `RegionalDnsServer`; resolve A and AAAA in the
tun; make DNS-over-TCP interception dual-stack instead of v4-only and
fail-closed; race each operator's v4 and v6 endpoint under the existing
`dohServerStagger`.

C6. Happy eyeballs everywhere. Generalize `dialInternalDohAddrs` into
`ConnectSettings.DialContext` for every hostname dial with an explicit
`FallbackDelay` of 250ms, and add a per-family QUIC handshake race for the
family-agnostic H3 path. In server, one shared dialer helper with the same
semantics replaces the bare `net.Dialer` in `DefaultHttpClient`, redis, the
exchange connection and the bare `http.Client` sites; `server/proxy` inherits
the tun fix.

### D. SDK, apps, tests

D1. Plumbing. `ProviderEvent.IpFamily` and `ExitInfo.IpFamily` in connect;
`ProviderGridPoint.IpFamily` (both copies), `ConnectedProviderLocation.
IpFamily`, `WindowStatus` category counts and `Ipv6Available` in the SDK;
cgo header regeneration; the three JS marshallers in `sdk/js`; the iOS RPC
bridge in `device_rpc.go`.

D2. UI. Under the transport distribution bar in each app's client-statistics
card: three wrapping rows labeled Both, v4, v6, one dot per provider, dot
diameter equal to the connect widget's live cell size (canvas width over grid
width). Provider rows show the category label. Apps: android
`ConnectStatsSections.kt`, apple `ConnectStatsSections.swift` with `FlowRow`,
windows `ConnectPage.cpp` with a new host under `TransportBarHost`, linux
`ConnectDrawer.cpp`, mmm `ConnectStats.jsx`.

D3. Tests. A shared dual-stack helper runs a test body for v4 and v6 and
requires IPv6 loopback; a host without it fails loudly. Tests run on
dual-stack hosts, not GitHub CI. Scope: every connect and server/connect test
that touches addresses, sockets or packets. Pure-logic tests are unchanged.

## 4. Wire and schema changes

| Surface | Change |
|---|---|
| H1 auth headers | `X-UR-IpFamily: 4|6` on pinned transports only |
| `protocol.Auth` | `int32 ip_family` (0 absent) |
| `POST /network/find-providers2` args | `ip_family`: `""`, `v4-capable`, `v6-capable`, `dualstack`, `v4-only`, `v6-only`; `""` means `v4-capable` |
| find-providers2 result provider | `ip_family`: `dualstack`, `v4-only`, `v6-only` |
| `network_client_connection` | `ip_version smallint`, `ip_family_intent smallint` |
| `network_client_location_reliability` | `ipv4_proven bool`, `ipv6_proven bool`; hash and location counts per A8 |
| redis `ClientScore` | `IpFamilies` bitmask, zero = v4-only |
| sdk `ProviderGridPoint`, `ConnectedProviderLocation`, `WindowStatus` | family fields per D1 |

All changes are backward compatible: old clients send no intent and no filter
and receive today's behavior; old cache entries read as v4-only.

## 5. Phases

1. Server (`../server`): migrations, tagging in `connect/transport.go` and
   `transport_announce.go`, aggregation and validity in
   `model/network_client_reliability_model.go`, score facet and filter in
   `model/network_client_location_model.go`, handler args. Deployable alone.
2. Infra: `services.yml` aliases, DNS records, warpctl regeneration.
   Prerequisite for enabling pinned transports.
3. Connect data plane: C1, C3, C4, C5, C6.
4. Connect control plane: A2, A4–A7 in `transport.go`, `control_family.go`,
   `net_http.go`; B1–B6 in `ip_remote_multi_client*.go`, `api.go`,
   `ip_mux_upgrade.go`; monitor field.
5. SDK and apps (`../sdk`, five app repos): A9 URL getters, provider
   transport creation in `device_local_provider.go` and `connectctl`, C2
   tunnel configuration, D1, D2.
6. Tests alongside every phase per D3, plus end-to-end: pinned transports
   tagged and categorized; find-providers2 filters and facets; window family
   minimums, starvation and local downgrade; AAAA filtering and ICMPv6
   no-route; tun v6; fragments and extension headers; DoH v6 racing.

## 6. Known limitations

- Same-network detection by address hash works only when the consumer and
  the provider are observed on the same family.
- A provider's category is the server's view at discovery time; B5 covers
  loss of v6 locally until the cache rebuilds.
- Categories depend on the reliability aggregation and score-cache cadence,
  so a family change on a provider lags by up to one rebuild.
- P2P transports are family-independent and are not part of the tag.
- A parked pinned transport (sleeping, idle by policy, or the held standby)
  keeps its required H1 budget claim and socket slot for its lifetime; only
  the optional H3 lease is yielded while parked.
