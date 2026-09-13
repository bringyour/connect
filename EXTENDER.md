# Extender plan

Status: design locked 2026-09-12 (section 2 onward). The original proposal
is kept verbatim in section 1. Three review rounds refined it; where a
bullet of the original is superseded, the decision that supersedes it says
so. Implementation runs in the phases of section 5, in order, each with
tests before the next starts.

## 1. Original proposal

- Extender speaks tcp 443, udp 443, dns and http3/dtls
- Match upstream connection IPv4/v6
- Pure proxy to whitelisted domains and dns unless extender header in outer connection . Only whitelisted inner tls connections are allowed to operator api/connect domains
- Operator maintains a list of extenders with active probes for uptime. If an extender fails a probe for too many failures it is removed until it re-actives itself.
- Operator publishes rotating geo dns with a sample of extender IPs. Use Rotue53 and extender.bringyour.com for our main operator. Match exteders to regions to publish a sample of the closest extenders per region
- Operator and each extender is a gossip network node
- App gets extender sample and connects to gossip network first before any other action
- App maintains a list of extenders from the gossip network
- The operator releases a slow drip of new extenders to the gossip the same cadence as the geo dns
- The network space can plug in the root gossip host/ip and extender dns name
- If an extender is tried and failed it is not retried for a timeout. It is put on a warning list. If it has only failed connections and no successful connections for a longer timeout it is removed entirely from the storage
- Extender list are durably stored in the sdk
- The SDK should expose the full list of known extenders, last usage, currently used extenders, and gossip network status (currently connected)
- The gossip network should fall under the app memory policy and the mobile apps should be clients only not allow peers to reduce memory usage
- The gossip network will notify of removed extenders. If an app notices itself removed and it wants to keep extending, it should activate again
- Use libp2p gossip protocol
- Each extender has a public key id that the operator signs with  root cert when the extender is activated
- The initial connect to the operator returns the root cert public key
- Every provider automatically tries to become an IPv4 and v6 extender using the operator acceptable api, which does a probe back and tests the extender public ips using the caller ip

Clarifications given during review, which the decisions below implement:

- The upstream from the extender to the operator is always TCP: tcp to
  extender, tcp to operator; udp to extender, tcp to operator; dns to
  extender, tcp to operator.
- "http3/dtls" means the TLS inside QUIC on the udp carrier. There is no
  DTLS carrier.
- The spoof domains connect bundles are also on the whitelist, so an active
  prober that speaks plain HTTPS to an extender gets the real site back.
  The extender protocol lives inside the outer TLS, where it is hard to
  discover on the wire.
- "dns" is the DNS-encoded carrier (the whodis packet translation). Queries
  that do not follow it are answered by a real forwarder over a trusted DoH
  resolver, for the same reason: probers must get realistic results.
- Low-memory apps take the feed only. Apps with more memory are full
  members of the gossip network, which strengthens it.

## 2. What already exists

- `extender/extender.go` is the v1 server: it terminates TLS on the
  configured tcp ports with a per-SNI self-signed RSA cert, reads a
  length-prefixed `protocol.ExtenderHeader`, checks the HMAC secret list
  and the allowed-host list, dials the destination and splices. A header
  that does not parse closes the connection. QUIC and udp variants are
  sketched in comments. `ExtenderSettings` has `Listen`, `DialContext` and
  `ErrorHandler` seams used by the tests.
- `net_extender.go` is the v1 client: `ExtenderConfig{Profile, Ip, Secret}`
  and `newExtenderDialTlsContext`, which dials the extender ip literal, runs
  the outer TLS with `InsecureSkipVerify` and TLS 1.3, writes the header,
  then runs the inner TLS to the destination. It plugs into `clientDialer`
  as a `DialTlsContextFunction`, so the platform websocket transport and
  the api client both go through it.
- `net_extender_profiles.go` enumerates random spoof profiles from
  `serviceHosts` and `mailHosts`, which are empty with a FIXME that wants
  the names kept out of the binary as plain strings. Random discovery
  therefore produces nothing today; only manually configured extenders
  work. `net_http.go` expands extenders from `ExtenderNetworks`,
  `ExtenderHostnames` resolved over DoH, and these profiles, and drops them
  after `ExtenderDropTimeout` without a success.
- `transport_pt.go` is the DNS packet translation. The client side
  (`PacketTranslationModeDns`) wraps a UDP socket so QUIC packets travel as
  DNS queries and answers; the server side (`PacketTranslationModeDecode53`)
  wraps a udp 53 listener. `handleDnsOther` receives queries that are not
  the translation and currently ignores them. The server's `listenH3Dns`
  in `server/connect/transport.go` runs a QUIC transport over the decode53
  translation with tld `ur.xyz.`; the client's `h3DialCandidates` in
  `transport_family.go` builds the matching client side.
- quic-go v0.61 `http3` exposes both halves of a stream takeover:
  `HTTPStreamer.HTTPStream()` on the server response writer and
  `ClientConn.OpenRequestStream` with `RequestStream` on the client.
- `sdk/network_space.go` carries `NetExtender` (manual ip and secret) and an
  unused `NetExtenderAutoConfigure`. `LocalState` persists JSON dot files
  such as `.provider_priors` through a store interface
  (`localStatePriorsStore`), which is the shape the extender store follows.
- `sdk/mobile_memory_policy.go` defines the low-memory profile: a mobile
  runtime with a memory target at or below 24 MiB.
- The server has `api-v4` and `api-v6` hosts (IPV6.md A9), so a request's
  caller address is of a known family. `session.ResolveClientAddress`
  yields the caller. `controller.GetLocationForIp` and `server.GetIpInfo`
  give a country for an ip. `aws-sdk-go` v1 is already a dependency.
  Taskworker jobs follow `taskworker/work/provider_egress_probe_work.go`.
  Handlers use `router.WrapWithInputRequireClient` for client-jwt calls.
  `HelloResult` returns `client_address`.
- IPV6.md A4: family-pinned provider transports are direct-only, so an
  extender never sits on a family-proving connection.

## 3. Decisions

### A. Carriers and the extender protocol

A1. Three carriers, one stream contract. The extender listens on tcp 443,
udp 443 and udp 53. Each carrier yields one reliable byte stream from the
client:

- tcp 443: TLS, terminated by the extender with a cert for the requested
  SNI (B3).
- udp 443: QUIC with ALPN `h3`, terminated the same way. The client's H3
  request stream is the byte stream.
- udp 53: the same QUIC server over the decode53 packet translation on the
  udp 53 socket, exactly `listenH3Dns`. The client wraps its UDP socket in
  the dns packet translation and dials QUIC to the extender ip on port 53,
  exactly `h3DialCandidates` for the h3dns mode. The client talks to the
  extender ip directly, so the pump variant does not apply. The encoding
  tld is carried in the record (B2) and defaults to `ur.xyz.`.

In every carrier the client sends one HTTP request carrying the extender
header (A3). On acceptance the stream is taken over and carries the inner
bytes: the inner TLS to the destination, or a reserved service (A8). The
upstream from the extender is always TCP on the client's family (A7). H3 to
the operator never crosses an extender; inside any carrier the platform
transport runs the H1 websocket over the inner TLS, which is today's
behavior for the tcp carrier. Ports are fixed per carrier; the old
multi-port personas are removed.

A2. Always terminated. There is no SNI splice. The outer TLS or QUIC is
terminated for every SNI so that the request inside can be inspected.

A3. The extender request. `POST /` with `Content-Type:
application/x-ur-extender` and a body of at most 1024 bytes holding the
serialized `ExtenderHeader`. On tcp it is an HTTP/1.1 request on the
terminated TLS connection; the extender's `http.Server` handler writes
`200` with the same content type and a body holding the response frame,
then hijacks the connection. The response frame is a 4-byte big-endian
length followed by the serialized `ExtenderResponse`, on every carrier:
on the udp carriers the response body and the raw bytes after it are one
http3 DATA stream, and a content length there would bound the reader the
client keeps, so the frame delimits itself and only the tcp response
carries a `Content-Length`. On quic and dns it is one
H3 request opened with `OpenRequestStream`; the handler writes the same
response and takes the stream with `HTTPStream()`. After the response both
ends use the stream raw. The client uses `http.Request.Write` and
`http.ReadResponse` on tcp, keeping the buffered reader for the bytes that
follow, and the `RequestStream` type on quic and dns, so both ends share
the http3 DATA framing. The client offers no ALPN on tcp, as today, so
HTTP/1.1 is negotiated; the server offers `h2` and `http/1.1` so a prober
that asks for h2 gets it. An extender request that arrives over h2 is
refused with 403, since h2 cannot be hijacked. Because the extender has
already read the first bytes of the stream to tell v1 from HTTP, the
served connection is no longer the `*tls.Conn` net/http's ALPN hook needs,
so h2 is dispatched directly through the configured `http2.Server`; the
SNI of a tcp connection is taken from the terminated connection state and
attached to the request context rather than read from `req.TLS`.

Legacy framing: when the first four bytes after the tcp handshake decode
as a big-endian length of at most 1024, the connection is handled as a v1
length-prefixed header with v1 semantics and no response frame. Any other
first bytes are prepended back and the connection goes to the HTTP server.
No HTTP method or TLS record begins with such a length, so the check is
unambiguous. v1 acceptance is dropped one release later. New clients send
v2 only, so a manual extender on an old binary must upgrade.

A4. Header v2. `ExtenderHeader` keeps `DestinationHost`, `DestinationPort`,
`Timestamp`, `Nonce` and `Signature` (the HMAC over timestamp and nonce for
private extenders with `allowedSecrets`) and gains `Challenge` (32 random
bytes, set by probes) and `Service` (0 forward, 1 gossip, 2 feed). The
response is `ExtenderResponse{PublicKey, ChallengeSignature, Carriers}`:
the extender's ed25519 public key or empty when it has none, the signature
over `"ur-extender-challenge-v1" || Challenge` when a challenge was given,
and the carriers this extender serves (`tcp`, `quic`, `dns`). Refusals are
HTTP 403 with no body: bad secret, destination not allowed, service not
available, header too large. The connection closes after a refusal. An
empty secret list means an open extender that accepts every header, which
is what an operator-activated extender is; a non-empty list requires the
HMAC to match one entry, which is the private extender of the network
space's manual configuration.

A5. Whitelist and fallback. The whitelist is the union of the bundled
spoof list (A10) and the operator domain patterns. Operator patterns come
from the extender's configuration; the sdk provider derives them from the
network space hosts as `<host>` and `*.<host>` for the host and the
migration host; connectctl takes them by flag. An extender request's
destination must match an operator pattern; spoof domains are never valid
destinations. Any other request on a terminated connection, HTTP/1.1, h2
or H3, is answered by a reverse proxy to `https://<sni>` when the SNI is
on the whitelist, using the extender's egress on the client's family with
normal CA verification upstream, so a prober gets the real site behind a
self-signed cert. A request whose SNI is not on the whitelist gets 403 and
the connection closes. A name is matched only when it is a syntactic host
name, since TLS accepts any bytes as an SNI and the name becomes the
upstream authority. Reverse proxy bounds: request body 1 MiB, relayed
bytes per connection 8 MiB, per-source concurrent 8, total 256, idle 30 s.
The body and concurrency bounds answer 503 before anything upstream is
opened; the relayed-bytes bound cuts the body, whose upstream content
length is therefore not relayed. `ExtenderSettings.SpoofDomains` overrides
the bundled list for tests and private deployments.

A6. udp 53. The decode53 translation carries the dns carrier. Every query
that is not the translation reaches a new `PacketTranslationSettings.
DnsOtherHandler(query []byte, addr net.Addr)` hook, which the extender sets
to a forwarder: parse the query, refuse `ANY`, resolve the question through
the extender's own DoH cache (`DohCache.Forward` for the raw response, with
the connect default DoH server list, overridable in `ExtenderSettings`),
rewrite the message id, and write the answer on the udp 53 socket
directly. Per-source limit 10 queries per second with burst 20, total 500
per second with the same burst, one in-flight query per source address, a
bounded worker pool of 64 so the translation's read loop never blocks,
a 5 s forward timeout, and a response cap of 4096 bytes with the TC bit
set when exceeded. No recursion of its own. A TXT query outside every
encoding tld is a forwarder query, not a translation query.

A7. Family match. The forward dial and the reverse proxy transport use
`tcp4` or `tcp6` by the family of the client's outer socket, so name
resolution yields only that family. The udp carriers use the family of the
datagram source. A destination with no address of that family fails the
request; the client moves to another extender.

A8. Reserved services. `Service` 1 (gossip) hands the taken-over stream to
the in-process gossip listener (D2). `Service` 2 (feed) hands it to the
feed server (D4). An extender without a gossip node refuses both with 403.
`DestinationHost` is ignored when `Service` is set. The handlers are
settings callbacks that own the stream for the duration of the call; the
extender closes the stream when the callback returns and keeps it in its
shutdown set meanwhile, so a listener implementation blocks in the
callback until its consumer releases the connection.

A9. Limits. Relay read and write timeouts 30 s as today. Header read
deadline 10 s, which also bounds the outer handshake and, as the HTTP
server's read timeout, the request body. Per-source concurrent connections 64, total 4096, both
settings. Idle QUIC connections close after 30 s.

A10. Spoof list and dial. `SpoofDomains()` in connect root returns the
bundled list, loaded from an embedded xor-masked gzip resource decoded on
first use, so the names do not appear as plain strings in the binary (the
existing FIXME). The content is the v1 service and mail name lists recovered from the
repository history (3523 names); tests install synthetic `.example` names
through a seam. The generator refuses to run without an input so it can
never rewrite the resource to an empty list by accident. A client dial picks one spoof domain per dialer at random. While the
bundled list is empty, a dial presents no SNI at all: the operator's name
must never appear in the outer ClientHello, and a TLS connection without
SNI is what an extender request to an ip literal looks like anyway.
`ExtenderProfile` becomes `{ConnectMode tcptls|quic|dns, ServerName, Port,
Fragment, Reorder, DnsTld}` and stays comparable; fragment and reorder apply
to tcp only. `ExtenderConfig` gains `PublicKey`. The dial per carrier: tcp
as today; quic dials `quic.Transport` with ALPN `h3` then
`http3.Transport.NewClientConn`; dns wraps the socket in the dns
translation first. Then A3, then the inner TLS as today. The quic and dns
carriers adapt the request stream to `net.Conn` (the commented
`streamConn`, revived).

### B. Identity, records and trust

B1. Keys. Ed25519 everywhere. An extender generates its identity key once;
the sdk persists it in local state as `.extender_key` (JSON with the hex
seed), connectctl reads it from `--extender_key_file` and creates it when
absent. The operator root key lives in the vault resource `extender.yml`
as `root_private_key_hex` with `root_public_keys_hex` listing every key
whose signatures are accepted, for rotation.

B2. Records. In `protocol/extender.proto`:

- `ExtenderAddress{Ip, IpVersion, Carriers []string}`, with `Ip` the text
  form of the address.
- `ExtenderRecordBody{PublicKey, Addresses, TcpPort, UdpPort, DnsPort,
  DnsTld, CountryCode, IssueTimeMs, ExpireTimeMs, NetworkHost}`.
- `ExtenderRecord{Body bytes, RootSignature, RootKeyId}` where `Body` is
  the serialized body, the signature is ed25519 over
  `"ur-extender-record-v1" || Body`, and `RootKeyId` is the first 8 bytes
  of sha256 of the signing public key.
- `ExtenderRevocationBody{PublicKey, IssueTimeMs, NetworkHost}` and
  `ExtenderRevocation{Body, RootSignature, RootKeyId}` with domain
  `"ur-extender-revocation-v1"`.
- `ExtenderGossipMessage{oneof record | revocation}`.
- `ExtenderFeedRequest{SampleCount, Subscribe}` and
  `ExtenderFeedFrame{oneof record | revocation | end_of_sample bool |
  keepalive bool}`.

Signing over the opaque serialized body avoids protobuf serialization
ambiguity. Verification and construction live in connect root
(`extender_record.go`) so the client, the mesh validators and the server
share one implementation. `NetworkHost` separates network spaces: a client
ignores records for a host that is neither its host nor its migration
host.

B3. Extender certificates. The identity key signs a self-signed ed25519 CA
cert (IsCA, ten years), regenerated when the key changes. One ECDSA P-256
leaf key is generated per process; a leaf per SNI is issued under the CA
on first sight with 30 days validity and cached (1024 entries). This
replaces the per-connection RSA-2048 `selfSign`. QUIC uses the same
callback. A client that knows the extender's key from a record sets
`VerifyPeerCertificate` to require that the leaf's signature verifies
under that key, keeping `InsecureSkipVerify` so no roots are consulted; a
client without a key skips verification as today. An extender without an
identity key issues per-SNI self-signed leaves.

B4. Root key distribution. The network space values carry
`ExtenderRootPublicKeys` (hex) as the trust anchor before first contact,
and `HelloResult` gains `extender_root_public_keys`. The hello list
replaces the stored list, since it arrives over the platform's pinned TLS
even through an untrusted extender. A record or revocation signed by a key
not in the current list is rejected.

B5. Directory semantics per key. Keep the newest record and the newest
revocation by issue time. A key is active when it has a record, the
record has not expired (5 minutes skew allowed) and no revocation with an
issue time at or after the record's issue time exists. A re-activation
therefore issues a record newer than any revocation.

### C. Operator: activation, probes, storage, publishing

C1. Tables.

```
network_extender (
    extender_id uuid PRIMARY KEY,
    network_id uuid NOT NULL,
    client_id uuid NOT NULL,
    public_key bytea NOT NULL UNIQUE,
    create_time timestamp NOT NULL,
    tcp_port int NOT NULL DEFAULT 443,
    udp_port int NOT NULL DEFAULT 443,
    dns_port int NOT NULL DEFAULT 53,
    dns_tld varchar NOT NULL DEFAULT 'ur.xyz.',
    country_code varchar NOT NULL DEFAULT '',
    active bool NOT NULL DEFAULT false,
    revoke_time timestamp NULL,
    record_issue_time timestamp NULL
)
network_extender_address (
    extender_id uuid NOT NULL,
    ip_version smallint NOT NULL,
    ip inet NOT NULL,
    carriers varchar NOT NULL,
    activate_time timestamp NOT NULL,
    last_probe_time timestamp NULL,
    last_probe_success_time timestamp NULL,
    consecutive_probe_failures int NOT NULL DEFAULT 0,
    active bool NOT NULL DEFAULT true,
    last_publish_time timestamp NULL,
    PRIMARY KEY (extender_id, ip_version)
)
network_extender_publish (
    publish_id uuid PRIMARY KEY,
    extender_id uuid NOT NULL,
    kind smallint NOT NULL,
    message bytea NOT NULL,
    create_time timestamp NOT NULL,
    published_time timestamp NULL
)
```

`carriers` is a comma-separated list. `kind` is 1 record, 2 revocation.
`message` is a serialized `ExtenderGossipMessage`. Indexes on
`network_extender_address (active, last_publish_time)` and
`network_extender_publish (published_time, create_time)`.

C2. Activation. `POST /network/extender-activate`, client jwt, on `api-v4`
or `api-v6` so the caller address has one family. An operator without
family hosts, such as a development operator on an ip literal, is
activated through its plain api url instead, and the family of the
outcome is the one the result reports. Args
`{public_key_hex, tcp_port, udp_port, dns_port, dns_tld, carriers}`. Rate
limit 6 per hour per client. The handler probes the caller ip synchronously
within 10 s: for each requested carrier, `connect.ProbeExtenderCarrier`
dials that carrier with a random spoof name, sends a header with a
challenge and verifies the response signature against `public_key_hex`;
then `connect.ProbeExtenderForward` performs a verified `GET /hello`
through the tcp carrier to the api url. Result `{activated, ip,
ip_version, carriers, error, expire_time, allowed_hosts, record,
bootstrap}`: `record` is the extender's own freshly signed record,
`bootstrap` up to 8 signed records of other active extenders, chosen at
random, `allowed_hosts` the operator patterns of A5. On success the row
and the family's address row are upserted active, `record_issue_time` is
set, and a record publish row is inserted. The country comes from the ip
geolocation of the caller. A probe failure returns `activated: false` with
the failing carrier in `error` and stores nothing. The tcp carrier is
required, since only it can prove the forward. Zero ports and an empty tld
take the C1 defaults. The configuration check precedes the rate limit so
an unconfigured operator never spends a caller's budget. A failed
geolocation leaves the country empty rather than failing the activation.
The bootstrap records are signed fresh at each activation. Probe requests
name the api host on port 443 as their destination.

C3. Uptime probes. Taskworker task every 5 minutes over every active
address, batched, concurrency 16, tcp challenge probe only, 15 s timeout.
A failure increments `consecutive_probe_failures`; at 6 the address is
deactivated. When an extender has no active address it becomes inactive,
`revoke_time` is set and a revocation publish row is inserted. A success
resets the counter and stamps `last_probe_success_time`. A deactivated
address is re-probed only by a new activation. The read of an extender's
remaining active addresses is locked, so two probes losing both families
of one extender in the same tick cannot each see the other still active
and leave the extender active with no address and no revocation.

C4. Publish tick. Taskworker task every 10 minutes. Drip: select up to 8
active extenders ordered by the oldest `last_publish_time` (nulls first),
sign a record for each with its active addresses, insert a record publish
row, stamp `last_publish_time`. When the active count exceeds what 8 per
tick rotates within 7 days, the batch grows to keep the rotation under 7
days so every record is republished before its 14 day expiry: the batch is
the larger of 8 and the active count divided by 1008 ticks, rounded up.
Each drip also stamps `record_issue_time`, since it is the newest record.
DNS: C5.

C5. Geo DNS. Route 53 geolocation routing by continent: AF, AN, AS, EU,
NA, OC, SA and the default, each an A and an AAAA set at
`extender.<host>` (env prefix rule of the network space for non-main
envs), TTL 60, up to 8 addresses per set sampled at random from active
addresses of that family in that continent and filled from the global set
when short; the default set samples globally. All sets go in one
`ChangeResourceRecordSets` UPSERT batch per tick; a set with no addresses
is deleted. No Route 53 health checks. Continent from country through a
static table in the server. Configuration in `extender.yml`:
`dns: {enabled, hosted_zone_name, hosted_zone_id, record_name, ttl,
sample_count, aws_region, aws_access_key_id, aws_secret_access_key}`. The
zone is named, and the id is resolved once per process through the aws
sdk's `ListHostedZonesByName` with the host's default credentials; an
explicit id overrides the name and static credentials override the chain.
The publisher is an interface with the aws-sdk-go implementation and a
fake for tests. The operator configuration names `bringyour.com` with the
record `extender.bringyour.com`.

C6. Gossip service. A new server package `gossip` with `cli/gossip`, one
replica. It runs a libp2p host (D1) with identity `gossip_identity_key_hex`
from `extender.yml`, listens with the websocket transport on its service
port behind nginx at `gossip.<host>` (a `gossip` entry in `services.yml`
with `websocket: true` and the alias exposed), joins the topic, and every
5 s selects unpublished rows (`FOR UPDATE SKIP LOCKED`, oldest first,
64 at a time), publishes each and stamps `published_time`. It never
originates records itself. The node is built with the member role, zero
peer target and no extender listener, with connection manager watermarks
of 512 and 1024 since every member and extender dials it. Its `services.yml`
entry carries `status: "no"`: the websocket listener owns the service port,
so there is no status route, and the service is watched through its logs.
The claim releases its row locks when the claim transaction commits, so
`SKIP LOCKED` partitions the queue between concurrent drains rather than
guaranteeing at-most-once delivery; a duplicate after a crash between claim
and mark is harmless to gossip, and the single replica makes it rare.

C6a. Gossip records. The `gossip_dns:` block of `extender.yml` lists,
per record, the zone name, the gossip name and a source name:
`{enabled, aws_region, records: [{hosted_zone_name, name, source_name}]}`.
A taskworker task runs at start and every 24 hours and mirrors the source
name's A and AAAA record sets, alias targets included, onto the gossip
name in its zone through the aws sdk, one change batch per zone, so
`gossip.bringyour.com` and `gossip.ur.network` follow `connect.<host>`
without operator steps; both zones are on Route 53. The certificate for
the gossip aliases comes from the existing warp certificate flow, which
issues for every alias in `services.yml`.

C7. Hello. `HelloResult.ExtenderRootPublicKeys []string` from
`root_public_keys_hex`, and `GossipPeerId string` (json
`gossip_peer_id`), the libp2p peer id of the operator node derived from
`gossip_identity_key_hex`; both empty when unconfigured. A member with no
peer id makes no operator dial.

### D. Gossip network

D1. Node (`connect/gossip`). go-libp2p host assembled from the swarm and
basic host with exactly the extender transport (D2) and the websocket
transport (the default transport set pulls in webtransport, which does
not build against the pinned quic-go, and would add quic, webtransport
and webrtc to every binary), noise security, yamux, gossipsub with strict
message signing and flood publish for locally originated messages (only
the operator originates, and without it a publish before the first
heartbeat graft is lost), topic `/ur/extender/<host>/1`, a validator that
decodes `ExtenderGossipMessage`, verifies the root signature against the
node's current key list and the `NetworkHost`, and rejects everything
else, peer scoring at library defaults, no discovery. Every accepted
message is applied to the directory (E1). Connection manager watermarks:
8/16 for members, 16/32 for extenders. `Publish` is exposed for the
operator node. The status is refreshed on a 5 s tick as well as on events,
since a peer's subscription event can precede the node's own stream to
it. Measured cost: 1.4 MiB on a darwin arm64 build of the whole sdk, so
no size ceiling changes. The node's own identity is the extender key when the node
is an extender, else a per-install key persisted with the extender key
file.

D2. Extender transport. A libp2p `transport.Transport` registered for
`/ip4|ip6/<ip>/tcp/443` addresses in place of the tcp transport. Dial runs
the connect root extender dial with `Service` gossip over the tcp carrier
to that ip on the tcp port the record names, verifying the outer cert
with the directory's key for that ip and refusing before any connection
when the expected peer id is not the one derived from that key, then the
upgrader (noise and yamux) with the expected peer id, which is derived
from the extender's ed25519 key. Listen exists only on
extenders: a listener fed by the extender's gossip accept channel (A8),
advertising `/ip4|ip6/<public ip>/tcp/443` per activated family. Apps have
no listener. The operator is dialed at `/dns/gossip.<host>/tcp/443/wss/p2p/<id>` by
the websocket transport (`/dns`, so a v6-only host resolves it too).

D3. Peering. Every node keeps a connection to the operator and to up to N
random active extenders from the directory, N 8 for members and 16 for
extenders, redialing on a 60 s tick as peers churn. Member apps behind NAT
are outbound-only and relay along the links they dial. No node presents a
record to join; validity is the root signature on every message plus peer
scoring, so a hostile member can only observe the drip and add bounded
load.

D4. Feed protocol (connect root, no libp2p). Over `Service` feed. The
client sends `ExtenderFeedRequest{SampleCount, Subscribe}`; the server
replies with up to `SampleCount` random active records, its own record
first when it has one, then `end_of_sample`, then, when `Subscribe` is
set, every record and revocation it applies until the client closes, with
a keepalive every 30 s. Frames are 4-byte length-prefixed protobuf of at
most 64 KiB. Server caps: sample 32, subscribers 256; a client over the
subscriber cap still gets its sample and then the stream ends, and a
subscriber that falls 64 frames behind is disconnected. The server reads
the stream so a departed client releases its slot at once. The feed server
lives in `connect/gossip` beside the node since it reads the same
directory; the feed client lives in connect root.

D5. Roles (sdk). The role is feed on the js build and when the mobile
low-memory policy holds, which is a mobile runtime with a process memory
budget at or below 24 MiB; otherwise member. A persisted setting
`ExtenderGossipMode` with values `auto`, `feed`, `member` overrides. Every
app takes a one-shot sample at start; the feed role keeps the subscribe
stream open, the member role runs the node instead.

D6. No full sync anywhere. gossipsub delivers live messages only, so a
joining node has no history and gets its initial state from a bounded
sample (D4, C2 bootstrap). The operator drips (C4), every node relays what
it hears, and nothing serves the full set.

### E. Client: directory, strategy, startup, storage

E1. Directory (`net_extender_directory.go`). Verified entries keyed by
public key hold the newest record and revocation (B5). Unverified entries
keyed by ip come from DNS bootstrap and manual configuration and hold no
key; they upgrade to verified when a record listing that ip arrives. Each
address carries local state: success and failure counts, last success,
last failure, first failure, consecutive failures, hold-until, last use,
in-use count. Policy, all settings: hold after a failure 10 minutes
doubling per consecutive failure to 6 hours; warning means consecutive
failures at least 1; removal when never succeeded and the first failure
is older than 24 hours, or when the last success is older than 7 days and
consecutive failures reach 3; expiry per record with 5 minutes skew;
revocation immediate; cap 512 entries, evicting expired, then
never-succeeded oldest first, then oldest last success. Manual entries are
never removed by policy. A `MonitorValue` publishes change; `Snapshot`
serves status. Persistence goes through a store interface `Load() ([]byte,
error)` and `Save([]byte) error` with a JSON envelope `{version, records,
addresses}`, saved coalesced at 1 s after a change.

E2. Strategy. `ClientStrategySettings.ExtenderDirectory` replaces
`ExtenderNetworks`, `ExtenderHostnames` and the profile enumeration, which
are removed along with `net_extender_profiles.go`. `expandExtenderDialers`
draws up to `ExpandExtenderProfileCount` candidates that are active and
not on hold, preferring verified over unverified and addresses of a family
the host has (`probeFamilySupport`), and creates one dialer per address
and carrier the record lists, priorities 100 tcp, 110 quic, 120 dns,
minimum weight `ExtenderMinimumWeight`. `clientDialer.Update` reports to
the directory. `collapseExtenderDialers` also drops dialers whose address
is held, revoked or removed, and judges the drop timeout from the later of
a dialer's creation and its last error, so a dialer that was expanded but
never tried survives to its first attempt. Expansion interleaves v4 and v6
candidates so a dual-stack host does not spend its budget on one family. `MaxExtenderCount`, `ExtenderDropTimeout`,
`ExtenderConfigs` and `SetCustomExtenders` keep their behavior; a manual
extender still excludes every other dialer.

E3. Network client (`net_extender_network.go`). Owns the refresh loop for
one network space: load the store; bootstrap by resolving `ExtenderDnsName`
A and AAAA over the strategy's DoH settings and adding the answers as
unverified addresses with source dns, with the system resolver as the
fallback when DoH fails; take a sample by dialing `Service` feed on a
candidate, verified first, tcp then quic then dns carriers, with
`SampleCount` 16 and `Subscribe` per role; apply the frames; mark the
initial sample done. In the feed role it keeps the stream and reconnects
through another candidate on failure with backoff 1 s doubling to 5
minutes. It re-bootstraps over DNS every 6 hours and whenever fewer than 4
active entries remain (held addresses count as active here; the startup
gate of E4 counts only usable ones), refreshes the root keys from hello
every 6 hours, and reconnects on network change. A subscribed stream that
is silent for 90 s, three keepalive intervals, is treated as gone. A
subscription that ends advances the backoff, which resets only after a
stream stayed up for the maximum backoff, so an extender that accepts,
samples and drops is not redialed every second. `Status()` reports feed
connected, the feed ip, last sample time, last error and whether the
initial attempt is done, which is set at `end_of_sample` so a served
sample releases the gate at once.

E4. Startup gate. `parallelEval` waits for the initial sample to complete
only while the directory has no usable entry and the network client is
still on its first attempt, for at most `ExtenderInitialSampleTimeout`
(2 s). A stored directory or a completed first attempt never waits.

E5. Outer verification. A dialer built from a verified record passes the
key into `ExtenderConfig.PublicKey` (B3).

### F. SDK surface and network space

F1. Network space values gain `ExtenderDnsName` (default `extender.<host>`
with the env prefix rule, `<env>-extender.<host>` for non-main envs),
`GossipUrl` (default `wss://gossip.<host>` with the same env prefix rule
but never the env secret path, since a multiaddr carries no path and the
gossip service has none) and
`ExtenderRootPublicKeys`. `NetExtenderAutoConfigure` and its getter are
removed; `NetExtender` stays. A url-only space, whose key host is not a
dotted name, derives its network host, extender dns name and gossip url
from the api url host by the shared label rule (`api.bringyour.com` gives
`bringyour.com` and `extender.bringyour.com`), and takes its root keys
from the bundled table for that derived host, so an embedder such as the
sn miner discovers and activates like a stored space. The bundled table
carries the operator's key under `bringyour.com` and `ur.network`. `NetworkSpace` constructs the directory, the
store at the space's local state directory as `.extenders` beside the
other dot files (memory only without a storage path),
the network client, and in the member role the gossip node, at
construction, and closes them with the space. cgo exports and js types are
regenerated.

F2. Status. `NetworkSpace.GetExtenderStatus() *ExtenderStatus` with
`Role`, `FeedConnected`, `FeedIp`, `GossipConnected` (at least one mesh
peer), `GossipPeerCount`, `KnownCount`, `ActiveCount`, `WarningCount`,
`HoldCount`, `LastSampleTime`, `LastError` and `Extenders
*ExtenderInfoList`; `ExtenderInfo` with `Id` (base58 of the key, empty
when unverified), `Ip`, `IpVersion`, `Carriers` (comma-separated, since
gomobile binds no string slice), `CountryCode`, `State`
(`active`, `warning`, `hold`, `unverified`, `revoked`, `expired`),
`Source` (`dns`, `feed`, `gossip`, `bootstrap`, `manual`),
`LastSuccessTime`, `LastFailureTime`, `SuccessCount`, `FailureCount`,
`InUse`, `ExpireTime`. `AddExtenderStatusChangeListener` coalesces to one
callback per second. `GetExtenderGossipMode` and `SetExtenderGossipMode`
persist D5.

F3. Provider status. `DeviceLocal.GetExtenderProvideStatus()
*ExtenderProvideStatus` with `Enabled`, `Listening`, `ListenError`,
`ActivatedV4`, `ActivatedV6`, `Ipv4`, `Ipv6`, `LastActivationTime`,
`LastActivationError`, `RevokedTime`, `ConnectionCount`, plus
`GetProvideExtender`, `SetProvideExtender` persisted in local state as
`.provide_extender` (default true), and a change listener. These follow
the `GetProviderFamilyTransportStatus` precedent on `DeviceLocal` only,
since the role exists only on desktop builds where the device is local.

### G. Provider extender role

G1. Eligibility. Compiled for desktop and connectctl only, build tags
`!ios && !android && !js`, so mobile binaries carry neither the extender
server nor the role. Default on with the opt-out of F3, plus an embedder
switch `DeviceLocalSettings.ProvideExtenderEnabled` for a process that runs
many providers, which the miner swarm turns off, and never on a hosted
device, which cannot provide.

G2. Lifecycle in `deviceLocalProvider`. When provide is on and the setting
is on: load or create the identity key, which belongs to the network space:
local state's `.extender_key` when the space has storage, otherwise the
seed an embedder passes through `DeviceLocalKeyMaterial`, otherwise one
the space generates and hands back through the same key material for the
embedder to persist (the miner keeps it beside its client key seed); start `extender.Server` on tcp
443, udp 443 and udp 53, each bound independently, a failed bind disabling
that carrier, with all failed meaning not listening; the forward dialer is
the device's egress-aware connect dial narrowed by family; the whitelist is
A5 from the space hosts plus the spoof list; the space's node is rebuilt
with the extender role, the in-process listener, the feed server and the
listen addresses of the activated families, so it becomes a listening
node. Bind failures log
once and are reflected in the provide status, never as a user-visible
error; a failed carrier stays down until the role restarts with provide
or the setting. The feed server is wired to the extender's feed handler,
and the node carries the in-process gossip listener; the node is rebuilt
whenever the set of activated addresses changes so no stale address is
advertised. When the user has chosen the feed-only gossip mode, the role
runs the server and the feed service without a node and refuses the gossip
service.

G3. Activation loop. At start, every 24 hours, and on triggers: own key
revoked as observed in the directory, the observed public address from
hello changing (checked hourly), and network change. Per family with a
global address (`FamilySupported`), `POST` activate to the family api
url with the client jwt through a direct-only client strategy, since an
activation that crossed an extender would publish the extender's address,
apply the bootstrap records to the directory, and record the status.
Backoff on failure 10 minutes doubling to 6 hours; a refusal of any
attempted family holds the whole pass and the retry reissues both, and a
pass that attempted nothing retries on the backoff rather than the daily
tick. A family without an address is skipped. The hourly address check is
one hello for both families and compares the address, not the port.

G4. connectctl gains `extender`, a standalone extender for operators and
tests: `--jwt`, `--api_url`, `--extender_key_file`, listen port flags,
`--allowed_host` repeated, `--state_dir`, running G2 and G3 without a
provider. It derives the network host from the api host by dropping the
service label and the extender dns name by replacing it, keeping an env
prefix, takes its whitelist from the api host patterns plus the flags,
does one synchronous hello at start to seed the root keys, and keeps its
key at the state directory when no key file is given or runs with an
ephemeral identity when there is neither.

### H. Packages and dependencies

- connect root: protocol changes, `extender_record.go`,
  `net_extender.go` (carriers, request, verification),
  `net_extender_spoof.go`, `net_extender_probe.go`,
  `net_extender_directory.go`, `net_extender_feed.go`,
  `net_extender_network.go`. No libp2p.
- `connect/gossip`: node, extender transport, feed server, in-process
  listener. Imports go-libp2p and go-libp2p-pubsub. Imported by the sdk
  except on js and by the server.
- `connect/extender`: server, carriers, HTTP handlers, reverse proxy, DNS
  forwarder, certificates, limits. It does not import `connect/gossip`:
  the reserved-service handlers are plain callbacks, and whoever runs both
  (the sdk provider role, connectctl) wires the listener and the feed
  server in. Built for desktop and connectctl.
- server: model, handlers, controller, taskworker work, Route 53
  publisher, `gossip` service, `cli/gossip`, migrations, hello.
- sdk: network space fields and lifecycle, status types, store, roles,
  provider role, local state settings, regenerated bindings.
- vault: `services.yml` gossip service and alias for main; `extender.yml`
  keys documented here, created by operations.
- Package layering per CODESTYLE: root never imports `gossip` or
  `extender`; `extender` imports `gossip`; both import root.

### I. Tests

Every phase ships tests with it. In-process fixtures only: the extender
seams, an httptest operator, libp2p mocknet or in-memory transports for
the mesh, a fake Route 53 publisher, a fake DoH forward, dual-stack
loopback for family matching (tests run on dual-stack hosts and require
v6), barriers rather than sleeps, synthetic names and RFC 5737 and
RFC 3849 addresses only. Server tests run under the repo's test harness
with the database.

## 4. Wire and schema changes

| Surface | Change |
|---|---|
| `protocol.ExtenderHeader` | `Challenge`, `Service` added |
| `protocol.ExtenderResponse` | new |
| `protocol.ExtenderAddress`, `ExtenderRecordBody`, `ExtenderRecord`, `ExtenderRevocationBody`, `ExtenderRevocation`, `ExtenderGossipMessage`, `ExtenderFeedRequest`, `ExtenderFeedFrame` | new |
| extender tcp 443 | HTTP/1.1 inside TLS; v1 framing accepted one release |
| extender udp 443, udp 53 | new carriers, H3 inside QUIC |
| `GET /hello` | `extender_root_public_keys` |
| `POST /network/extender-activate` | new |
| `network_extender`, `network_extender_address`, `network_extender_publish` | new tables |
| Route 53 `extender.<host>` | geolocation A and AAAA sets |
| gossipsub topic `/ur/extender/<host>/1` | new |
| `sdk.NetworkSpaceValues` | `ExtenderDnsName`, `GossipUrl`, `ExtenderRootPublicKeys` added; `NetExtenderAutoConfigure` removed |
| sdk local state | `.extenders`, `.extender_key`, `.provide_extender`, gossip mode |
| `connect.ClientStrategySettings` | `ExtenderDirectory`, `ExtenderInitialSampleTimeout` added; `ExtenderNetworks`, `ExtenderHostnames` removed |

Old clients keep working: the header's new fields are optional, the hello
field is additive, the tables are new, and a v1 extender client still
reaches a v2 extender for one release.

## 5. Phases

Each phase: the design section is the contract, the implementation lands
with its tests, the whole package test suite of every touched module
passes, and a review closes it before the next starts. Phases 2 and 3 are
independent and may run concurrently in their separate repositories.
Phase 5b follows 4 because both touch the server.

1. Extender protocol v2 (connect root, `connect/extender`, protocol): A1
   to A10, B1 to B3, `ProbeExtenderCarrier` and `ProbeExtenderForward`.
   Acceptance: a client reaches an in-process operator through each of the
   three carriers over v4 and over v6 with the forward on the matching
   family; a v1-framed client still works on tcp; a plain HTTPS, h2 and H3
   GET with a whitelisted SNI gets the reverse-proxied fixture site and a
   non-whitelisted SNI gets 403; an extender request to a spoof domain
   destination is refused; udp 53 non-translation queries are answered
   from the fake DoH forward, ANY is refused, rate limits hold; the
   challenge response verifies and a wrong key fails; the outer cert
   verifies against the record key and a substituted chain fails; the
   limits of A9 hold under a flood.
2. Server: C1, C2, C3, C7 and the drip half of C4. Acceptance: activation
   against an in-process extender stores the rows and returns a signed
   record, bootstrap and allowed hosts; a failed carrier rejects; the rate
   limit holds; probes deactivate after 6 failures and insert a
   revocation; a re-activation issues a newer record; the drip rotates
   oldest first and grows the batch under load; hello returns the keys.
3. Client: E1 to E5, F1, F2, D5 role selection without the node (feed
   role for every app in this phase). Acceptance: the directory policy
   from E1 is pinned by tests for each transition; the strategy uses
   directory candidates and reports outcomes; the store round-trips and
   tolerates corruption; DNS bootstrap over an in-process DoH server
   yields unverified entries that upgrade on a feed record; the startup
   gate waits at most 2 s and never with a stored directory; the sdk
   status and listener reflect every change; the env prefix rule for the
   defaults; bindings regenerate.
4. Route 53 publisher: C5. Acceptance: continent sampling, fill from
   global, deletion of empty sets, one batch per tick, the fake publisher
   records exact sets, configuration disabled leaves DNS untouched.
5. Gossip. 5a (connect, sdk): D1 to D4, the member role, the feed server
   on extenders, the gossip listener in `connect/extender`. Acceptance:
   three extenders and a member on in-memory transports relay a signed
   record and a revocation to every directory and reject a forged one;
   the feed serves a sample and streams updates; the extender transport
   dials through the tcp carrier and the upgrader authenticates the peer
   id; the js build stays feed-only; the iOS size ceiling is measured and
   raised as a reviewed change if needed. 5b (server, vault): C6 and the
   gossip service. Acceptance: publish rows drain in order and are
   stamped; the service reconnects; a second replica does not double
   publish.
6. Provider extender role: G1 to G4. Acceptance: a desktop provider binds,
   activates over v4 and v6 against an in-process operator, appears in its
   own directory, re-activates on an observed revocation and on an
   address change, honors the opt-out, and skips bind failures silently;
   connectctl extender runs the same path.

## 6. Known limitations

- Extender relay traffic is not attributed or credited. PROXY-protocol
  attribution to a dedicated edge port is a later phase.
- The reverse proxy answers probers behind a self-signed cert; an active
  prober that validates certificates sees a misconfigured host, which is
  the accepted posture.
- H3 to the operator is unavailable through an extender; the H1 websocket
  runs inside the carriers.
- Records lag reality by up to one drip rotation; the directory's local
  failure policy covers the gap.
- An extender behind NAT or without a public address never activates,
  which the probe-back guarantees silently.
- The spoof list ships empty until operations provide it, so extender
  dialers appear only after that.

## 7. As built

All phases were implemented and committed on branch `extender` on
2026-09-12: connect (protocol, carriers, records, probes, directory, feed,
network client, gossip node, activator, connectctl), server (tables,
activation, probes, drip, Route 53, gossip service, hello), sdk (network
space values, status, roles, provider role, bindings), vault (the gossip
service entry), build (the gossip build step), operator-proxy (go.sum).
Each phase's refinements are recorded inline above.

Verification at the end: connect `go test ./...` green; sdk full suite green
except `TestDeviceLocalProviderMemoryUnderLoad`, which fails on the build
host before this work (31.3 to 31.7 MiB against a 31.0 MiB ceiling); server
model, api, taskworker and gossip suites green, and the controller suite
showing only its 15 pre-existing failures in the ARIN and account
reconcile tests; the js and mobile builds compile. The libp2p dependency
costs 1.4 MiB of binary.

Done the same day: the spoof list is bundled, `vault/main/extender.yml`
holds the generated root and gossip keys, the network hosts, the api url,
the named zone and the gossip record block, the sdk bundles the root
public key, the zone is resolved by name and the gossip records are
mirrored by the server through the aws sdk with the host's credentials,
and the sn miner runs the role with a persisted identity. What remains
for operations: aws credentials on the taskworker hosts with Route 53
access to both zones, no plain A or AAAA record at
`extender.bringyour.com`, and a deploy of the gossip service, whose
aliases the warp certificate flow already covers.
